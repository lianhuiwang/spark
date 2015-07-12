package org.apache.spark.sql.catalyst.expressions;

import com.google.common.annotations.VisibleForTesting;

import scala.math.Ordering;

import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.bitset.BitSet;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.map.HashMapGrowthStrategy;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.MemoryLocation;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import org.apache.spark.util.collection.Sorter;
import org.apache.spark.sql.catalyst.util.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.util.collection.SortDataFormat;

import java.util.Iterator;
import java.util.List;
import java.util.Comparator;

/**
 * An append-only hash map where keys and values are contiguous regions of bytes.
 * <p>
 * This is backed by a power-of-2-sized hash table, using quadratic probing with triangular numbers,
 * which is guaranteed to exhaust the space.
 * <p>
 * The map can support up to 2^29 keys. If the key cardinality is higher than this, you should
 * probably be using sorting instead of hashing for better cache locality.
 * <p>
 * This class is not thread safe.
 */
public class UnsafeAppendOnlyMap {

  /**
   * The maximum number of keys that BytesToBytesMap supports. The hash table has to be
   * power-of-2-sized and its backing Java array can contain at most (1 << 30) elements, since
   * that's the largest power-of-2 that's less than Integer.MAX_VALUE. We need two long array
   * entries per key, giving us a maximum capacity of (1 << 29).
   */
  @VisibleForTesting
  static final int MAX_CAPACITY = (1 << 29);

  private static final Murmur3_x86_32 HASHER = new Murmur3_x86_32(0);

  private static final HashMapGrowthStrategy growthStrategy = HashMapGrowthStrategy.DOUBLING;

  private final TaskMemoryManager memoryManager;

  /**
   * Special record length that is placed after the last record in a data page.
   */
  private static final int END_OF_PAGE_MARKER = -1;

  /**
   * A single array to store the key and value.
   * <p/>
   * Position {@code 2 * i} in the array is used to track a pointer to the key at index {@code i},
   * while position {@code 2 * i + 1} in the array holds key's full 32-bit hashcode.
   */
  private long[] longArray;

  /**
   * A {@link BitSet} used to track location of the map where the key is set.
   * Size of the bitset should be half of the size of the long array.
   */
  private BitSet bitset;

  private final double loadFactor;

  /**
   * Number of keys defined in the map.
   */
  private int size;

  /**
   * The map will be expanded once the number of keys exceeds this threshold.
   */
  private int growthThreshold;

  private int capacity;

  /**
   * Mask for truncating hashcodes so that they do not exceed the long array's size.
   * This is a strength reduction optimization; we're essentially performing a modulus operation,
   * but doing so with a bitmask because this is a power-of-2-sized hash map.
   */
  private int mask;

  /**
   * Return value of {@link UnsafeAppendOnlyMap#lookup(Object, long, int)}.
   */
  private final Location loc;

  private final boolean enablePerfMetrics;
  private long timeSpentResizingNs = 0;

  private long numKeyLookups = 0;
  private long numProbes = 0;
  private long numHashCollisions = 0;

  public UnsafeAppendOnlyMap(
      TaskMemoryManager memoryManager,
      int initialCapacity,
      double loadFactor,
      boolean enablePerfMetrics) {
    this.memoryManager = memoryManager;
    this.loadFactor = loadFactor;
    this.loc = new Location();
    this.enablePerfMetrics = enablePerfMetrics;

    if (initialCapacity <= 0) {
      throw new IllegalArgumentException("Initial capacity must be greater than 0");
    }
    if (initialCapacity > MAX_CAPACITY) {
      throw new IllegalArgumentException("Initial capacity " + initialCapacity
              + " exceeds maximum capacity of " + MAX_CAPACITY);
    }
    // The capacity needs to be divisible by 64 so that our bit set can be sized properly
    capacity = Math.max((int) Math.min(MAX_CAPACITY, nextPowerOf2(initialCapacity)), 64);
    allocate(capacity);
  }

  public UnsafeAppendOnlyMap(
      TaskMemoryManager memoryManager,
      int initialCapacity,
      boolean enablePerfMetrics) {
    this(memoryManager, initialCapacity, 0.70, enablePerfMetrics);
  }

  private static final class BytesToBytesMapIterator
      implements Iterator<Location> {

    private final int numRecords;
    private final Iterator<MemoryBlock> dataPagesIterator;
    private final Location loc;

    private int currentRecordNumber = 0;
    private Object pageBaseObject;
    private long offsetInPage;

    BytesToBytesMapIterator(int numRecords,
        Iterator<MemoryBlock> dataPagesIterator, Location loc) {
      this.numRecords = numRecords;
      this.dataPagesIterator = dataPagesIterator;
      this.loc = loc;
      if (dataPagesIterator.hasNext()) {
        advanceToNextPage();
      }
    }

    private void advanceToNextPage() {
      final MemoryBlock currentPage = dataPagesIterator.next();
      pageBaseObject = currentPage.getBaseObject();
      offsetInPage = currentPage.getBaseOffset();
    }

    @Override
    public boolean hasNext() {
      return currentRecordNumber != numRecords;
    }

    @Override
    public Location next() {
      int keyLength = (int) PlatformDependent.UNSAFE.getLong(pageBaseObject, offsetInPage);
      if (keyLength == END_OF_PAGE_MARKER) {
        advanceToNextPage();
        keyLength = (int) PlatformDependent.UNSAFE.getLong(pageBaseObject, offsetInPage);
      }
      loc.with(pageBaseObject, offsetInPage);
      offsetInPage += 8 + 8 + keyLength + loc.getValueLength();
      currentRecordNumber++;
      return loc;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Returns an iterator for iterating over the entries of this map.
   * <p/>
   * For efficiency, all calls to `next()` will return the same {@link Location} object.
   * <p/>
   * If any other lookups or operations are performed on this map while iterating over it, including
   * `lookup()`, the behavior of the returned iterator is undefined.
   */
  public Iterator<Location> iterator(List<MemoryBlock> dataPages) {
    return new BytesToBytesMapIterator(size, dataPages.iterator(), loc);
  }

  private static final class SortComparator implements Comparator<Long> {

    private final TaskMemoryManager memoryManager;
    private final Ordering<InternalRow> ordering;
    private final int numFields;
    private final ObjectPool objPool;
    private final UnsafeRow row1 = new UnsafeRow();
    private final UnsafeRow row2 = new UnsafeRow();

    SortComparator(TaskMemoryManager memoryManager,
        Ordering<InternalRow> ordering, int numFields, ObjectPool objPool) {
      this.memoryManager = memoryManager;
      this.numFields = numFields;
      this.ordering = ordering;
      this.objPool = objPool;
    }

    @Override
    public int compare(Long r1, Long r2) {
        final Object baseObject1 = memoryManager.getPage(r1);
        final long baseOffset1 = memoryManager.getOffsetInPage(r1) + 8;

        final Object baseObject2 = memoryManager.getPage(r2);
        final long baseOffset2 = memoryManager.getOffsetInPage(r2) + 8; // skip length

        //System.out.println("r1.pointer=" + r1.recordPointer + ",r2.pointer=" + r2.recordPointer);
        row1.pointTo(baseObject1, baseOffset1, numFields, objPool);
        row2.pointTo(baseObject2, baseOffset2, numFields, objPool);
        //System.out.println("row1=" + row1 + ",row2=" + row2);
        int comp = ordering.compare(row1, row2);
        //int comp = row1.getInt(0) - row2.getInt(0);
//        System.out.println("row1=" + row1.getInt(0) + ",row2=" + row2.getInt(0)
//            +" ,comp=" + comp);
        return (comp < 0) ? -1 : (comp > 0) ? 1 : 0;
    }
  }

  private static final class KVArraySortDataFormat extends SortDataFormat<Long, long[]> {

    @Override
    public Long getKey(long[] data, int pos) {
      return (Long)data[2 * pos];
    }

    @Override
    public Long newKey() {
      return 0L;
    }

    @Override
    public void swap(long[] data,int pos0, int pos1) {
      long tmpKey = data[2 * pos0];
      long tmpVal = data[2 * pos0 + 1];
      data[2 * pos0] = data[2 * pos1];
      data[2 * pos0 + 1] = data[2 * pos1 + 1];
      data[2 * pos1] = tmpKey;
      data[2 * pos1 + 1] = tmpVal;
    }

    @Override
    public void copyElement(long[] src, int srcPos, long[] dst, int dstPos) {
      dst[2 * dstPos] = src[2 * srcPos];
      dst[2 * dstPos + 1] = src[2 * srcPos + 1];
    }

    @Override
    public void copyRange(long[] src, int srcPos, long[] dst, int dstPos, int length) {
      System.arraycopy(src, 2 * srcPos, dst, 2 * dstPos, 2 * length);
    }

    @Override
    public long[] allocate(int length) {
        return new long[2 * length];
    }
  }

  public UnsafeMapSorterIterator getSortedIterator(List<MemoryBlock> dataPages,
      Ordering<InternalRow> ordering,
      int numFields, ObjectPool objPool) {
    Comparator<Long> sortComparator = new SortComparator(this.memoryManager,
        ordering, numFields, objPool);
    // Pack KV pairs into the front of the underlying array
    int keyIndex = 0;
    int newIndex = 0;
    while (keyIndex < capacity) {
      if (bitset.isSet(keyIndex)) {
        longArray[2 * newIndex] = longArray[2 * keyIndex];
        longArray[2 * newIndex + 1] = longArray[2 * keyIndex + 1];
        newIndex += 1;
      }
      keyIndex += 1;
    }
    //System.out.println("newIndex==" + newIndex);
    Sorter<Long, long[]> sorter = new Sorter<>(new KVArraySortDataFormat());
    sorter.sort(longArray, 0, newIndex, sortComparator);
//    Sorter<Long, long[]> sorter =
//        new Sorter<>(new KVArraySortDataFormat());
//    sorter.sort(longArray, 0, newIndex, new KeyHashCodeComparator());
    return new UnsafeMapSorterIterator(newIndex, longArray, this.loc);
  }

  /**
   * An iterator-like class that's used instead of Java's Iterator in order to facilitate inlining.
   */
  public static final class UnsafeMapSorterIterator implements Iterator<Location> {

    private final long[] pointerArray;
    private final int numRecords;
    private int currentRecordNumber = 0;
    private final Location loc;

    public UnsafeMapSorterIterator(int numRecords, long[] pointerArray,
        Location loc) {
      this.numRecords = numRecords;
      this.pointerArray = pointerArray;
      this.loc = loc;
    }

    @Override
    public boolean hasNext() {
      return currentRecordNumber != numRecords;
    }

    @Override
    public Location next() {
      loc.with(pointerArray[currentRecordNumber * 2]);
      currentRecordNumber++;
      return loc;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Allocate new data structures for this map. When calling this outside of the constructor,
   * make sure to keep references to the old data structures so that you can free them.
   *
   * @param capacity the new map capacity
   */
  private void allocate(int capacity) {
    assert (capacity >= 0);
    assert (capacity <= MAX_CAPACITY);
    //longArray = new LongArray(memoryManager.allocate(capacity * 8L * 2));
    longArray = new long[capacity * 2];
    System.out.println("capacity=" + (capacity * 2));
    bitset = new BitSet(MemoryBlock.fromLongArray(new long[capacity / 64]));

    this.growthThreshold = (int) (capacity * loadFactor);
    this.mask = capacity - 1;
  }

  public boolean hasSpaceForAnotherRecord() {
    return size < growthThreshold;
  }

  public long getMemoryUsage() {
    return capacity * 8L * 2 + capacity / 8;
  }

  public static long getMemoryUsage(int initialCapacity) {
    // The capacity needs to be divisible by 64 so that our bit set can be sized properly
    int nextCapacity =
        Math.max((int) Math.min(MAX_CAPACITY, nextPowerOf2(initialCapacity)),
            64);
    return nextCapacity * 8L * 2 + nextCapacity / 8;
  }

  public long getGrowMemoryUsage() {
    int nextCapacity =
        Math.min(growthStrategy.nextCapacity(capacity), MAX_CAPACITY);
    // The capacity needs to be divisible by 64 so that our bit set can be sized properly
    nextCapacity =
        Math.max((int) Math.min(MAX_CAPACITY, nextPowerOf2(nextCapacity)), 64);
    return nextCapacity * 8L * 2 + nextCapacity / 8;
  }

  /**
   * Free all allocated memory associated with this map, including the storage for keys and values
   * as well as the hash map array itself.
   * <p/>
   * This method is idempotent.
   */
  public void free() {
    if (longArray != null) {
      //memoryManager.free(longArray.memoryBlock());
      longArray = null;
    }
    if (bitset != null) {
      // The bitset's heap memory isn't managed by a memory manager, so no need to free it here.
      bitset = null;
    }
  }

  /**
   * Returns the total amount of memory, in bytes, consumed by this map's managed structures.
   */
  public long getTotalMemoryConsumption() {
    return (bitset.memoryBlock().size() + longArray.length * 8);
  }

  /**
   * Returns the total amount of time spent resizing this map (in nanoseconds).
   */
  public long getTimeSpentResizingNs() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException();
    }
    return timeSpentResizingNs;
  }

  /**
   * Returns the average number of probes per key lookup.
   */
  public double getAverageProbesPerLookup() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException();
    }
    return (1.0 * numProbes) / numKeyLookups;
  }

  public long getNumHashCollisions() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException();
    }
    return numHashCollisions;
  }

  /**
   * Handle returned by {@link UnsafeAppendOnlyMap#lookup(Object, long, int)} function.
   */
  public final class Location {
    /**
     * An index into the hash map's Long array
     */
    public int pos;
    /**
     * True if this location points to a position where a key is defined, false otherwise
     */
    private boolean isDefined;
    /**
     * The hashcode of the most recent key passed to
     * {@link UnsafeAppendOnlyMap#lookup(Object, long, int)}. Caching this hashcode here allows us to
     * avoid re-hashing the key when storing a value for that key.
     */
    private int keyHashcode;
    private final MemoryLocation keyMemoryLocation = new MemoryLocation();
    private final MemoryLocation valueMemoryLocation = new MemoryLocation();
    private int keyLength;
    private int valueLength;

    private void updateAddressesAndSizes(long fullKeyAddress) {
      updateAddressesAndSizes(
          memoryManager.getPage(fullKeyAddress),
          memoryManager.getOffsetInPage(fullKeyAddress));
    }

    private void updateAddressesAndSizes(Object page, long keyOffsetInPage) {
      long position = keyOffsetInPage;
      keyLength = (int) PlatformDependent.UNSAFE.getLong(page, position);
      position += 8; // word used to store the key size
      keyMemoryLocation.setObjAndOffset(page, position);
      position += keyLength;
      valueLength = (int) PlatformDependent.UNSAFE.getLong(page, position);
      position += 8; // word used to store the key size
      //System.out.println("keyLength=" + keyLength + " ,valueLength=" + valueLength);
      valueMemoryLocation.setObjAndOffset(page, position);
    }

    Location with(int pos, int keyHashcode, boolean isDefined) {
      this.pos = pos;
      this.isDefined = isDefined;
      this.keyHashcode = keyHashcode;
      if (isDefined) {
        final long fullKeyAddress = longArray[pos * 2];
        updateAddressesAndSizes(fullKeyAddress);
      }
      return this;
    }

    Location with(long fullKeyAddress) {
      this.isDefined = true;
      //System.out.println("fullKeyAddress=" + fullKeyAddress);
      updateAddressesAndSizes(fullKeyAddress);
      return this;
    }

    Location with(Object page, long keyOffsetInPage) {
      this.isDefined = true;
      updateAddressesAndSizes(page, keyOffsetInPage);
      return this;
    }

    /**
     * Returns true if the key is defined at this position, and false otherwise.
     */
    public boolean isDefined() {
      return isDefined;
    }

    /**
     * Returns the address of the key defined at this position.
     * This points to the first byte of the key data.
     * Unspecified behavior if the key is not defined.
     * For efficiency reasons, calls to this method always returns the same MemoryLocation object.
     */
    public MemoryLocation getKeyAddress() {
      assert (isDefined);
      return keyMemoryLocation;
    }

    /**
     * Returns the length of the key defined at this position.
     * Unspecified behavior if the key is not defined.
     */
    public int getKeyLength() {
      assert (isDefined);
      return keyLength;
    }

    /**
     * Returns the address of the value defined at this position.
     * This points to the first byte of the value data.
     * Unspecified behavior if the key is not defined.
     * For efficiency reasons, calls to this method always returns the same MemoryLocation object.
     */
    public MemoryLocation getValueAddress() {
      assert (isDefined);
      return valueMemoryLocation;
    }

    /**
     * Returns the length of the value defined at this position.
     * Unspecified behavior if the key is not defined.
     */
    public int getValueLength() {
      assert (isDefined);
      return valueLength;
    }

  }

  /**
   * Looks up a key, and return a {@link Location} handle that can be used to test existence
   * and read/write values.
   * <p/>
   * This function always return the same {@link Location} instance to avoid object allocation.
   */
  public Location lookup(
      Object keyBaseObject,
      long keyBaseOffset,
      int keyRowLengthBytes) {
    if (enablePerfMetrics) {
      numKeyLookups++;
    }
    final int hashcode =
        HASHER.hashUnsafeWords(keyBaseObject, keyBaseOffset, keyRowLengthBytes);
    int pos = hashcode & mask;
    int step = 1;
    while (true) {
      if (enablePerfMetrics) {
        numProbes++;
      }
      if (!bitset.isSet(pos)) {
        // This is a new key.
        //System.out.println("pos =" + pos + ",mask=" + mask);
        return loc.with(pos, hashcode, false);
      } else {
        long stored = longArray[pos * 2 + 1];
        if ((int) (stored) == hashcode) {
          // Full hash code matches.  Let's compare the keys for equality.
          loc.with(pos, hashcode, true);
          if (loc.getKeyLength() == keyRowLengthBytes) {
            final MemoryLocation keyAddress = loc.getKeyAddress();
            final Object storedKeyBaseObject = keyAddress.getBaseObject();
            final long storedKeyBaseOffset = keyAddress.getBaseOffset();
            final boolean areEqual = ByteArrayMethods.wordAlignedArrayEquals(
                keyBaseObject,
                keyBaseOffset,
                storedKeyBaseObject,
                storedKeyBaseOffset,
                keyRowLengthBytes
            );
            if (areEqual) {
              return loc;
            } else {
              if (enablePerfMetrics) {
                numHashCollisions++;
              }
            }
          }
        }
      }
      pos = (pos + step) & mask;
      step++;
    }
  }

  /**
   * Store a new key and value. This method may only be called once for a given key; if you want
   * to update the value associated with a key, then you can directly manipulate the bytes stored
   * at the value address.
   * <p/>
   * It is only valid to call this method immediately after calling `lookup()` using the same key.
   * <p/>
   * The key and value must be word-aligned (that is, their sizes must multiples of 8).
   * <p/>
   * After calling this method, calls to `get[Key|Value]Address()` and `get[Key|Value]Length`
   * will return information on the data stored by this `putNewKey` call.
   * <p/>
   * As an example usage, here's the proper way to store a new key:
   * <p/>
   * <pre>
   *   Location loc = map.lookup(keyBaseObject, keyBaseOffset, keyLengthInBytes);
   *   if (!loc.isDefined()) {
   *     loc.putNewKey(keyBaseObject, keyBaseOffset, keyLengthInBytes, ...)
   *   }
   * </pre>
   * <p/>
   * Unspecified behavior if the key is not defined.
   */
  public void putNewKey(
      long storedKeyAddress,
      Location location) {
    if (size == MAX_CAPACITY) {
      throw new IllegalStateException(
          "BytesToBytesMap has reached maximum capacity");
    }
    size++;
    bitset.set(location.pos);

    //System.out.println("location.pos==" + location.pos);
    longArray[location.pos * 2] = storedKeyAddress;
    longArray[location.pos * 2 + 1] = location.keyHashcode;
    System.out.println("storedKeyAddress==" + storedKeyAddress);
    location.updateAddressesAndSizes(storedKeyAddress);
    location.isDefined = true;
    if (size > growthThreshold && longArray.length < MAX_CAPACITY) {
      growAndRehash();
    }
  }

  /**
   * Grows the size of the hash table and re-hash everything.
   */
  @VisibleForTesting void growAndRehash() {
    long resizeStartTime = -1;
    if (enablePerfMetrics) {
      resizeStartTime = System.nanoTime();
    }
    // Store references to the old data structures to be used when we re-hash
    final long[] oldLongArray = longArray;
    final BitSet oldBitSet = bitset;
    final int oldCapacity = (int) oldBitSet.capacity();

    int nextCapacity =
        Math.min(growthStrategy.nextCapacity(oldCapacity), MAX_CAPACITY);
    // The capacity needs to be divisible by 64 so that our bit set can be sized properly
    capacity =
        Math.max((int) Math.min(MAX_CAPACITY, nextPowerOf2(nextCapacity)), 64);
    // Allocate the new data structures
    allocate(capacity);

    // Re-mask (we don't recompute the hashcode because we stored all 32 bits of it)
    for (int pos = oldBitSet.nextSetBit(0);
         pos >= 0; pos = oldBitSet.nextSetBit(pos + 1)) {
      final long keyPointer = oldLongArray[pos * 2];
      final int hashcode = (int) oldLongArray[pos * 2 + 1];
      int newPos = hashcode & mask;
      int step = 1;
      boolean keepGoing = true;

      // No need to check for equality here when we insert so this has one less if branch than
      // the similar code path in addWithoutResize.
      while (keepGoing) {
        if (!bitset.isSet(newPos)) {
          bitset.set(newPos);
          longArray[newPos * 2] = keyPointer;
          longArray[newPos * 2 + 1] = hashcode;
          keepGoing = false;
        } else {
          newPos = (newPos + step) & mask;
          step++;
        }
      }
    }

    // Deallocate the old data structures.
    //memoryManager.free(oldLongArray.memoryBlock());
    if (enablePerfMetrics) {
      timeSpentResizingNs += System.nanoTime() - resizeStartTime;
    }
  }

  /**
   * Returns the next number greater or equal num that is power of 2.
   */
  private static long nextPowerOf2(long num) {
    final long highBit = Long.highestOneBit(num);
    return (highBit == num) ? num : highBit << 1;
  }
}
