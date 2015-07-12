/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.MutableProjection;
import org.apache.spark.sql.catalyst.util.ObjectPool;
import org.apache.spark.sql.catalyst.util.UniqueObjectPool;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.*;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.MemoryLocation;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import org.apache.spark.util.Utils;
import org.apache.spark.serializer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Tuple2;
import scala.math.Ordering;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.*;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Unsafe-based HashMap for performing aggregations where the aggregated values are fixed-width.
 */
public final class UnsafeFixedWidthExternalAggregation {

  private final Logger logger =
      LoggerFactory.getLogger(UnsafeFixedWidthExternalAggregation.class);
  /**
   * Special record length that is placed after the last record in a data page.
   */
  private static final int END_OF_PAGE_MARKER = -1;

  private final int initialCapacity;

  private final TaskMemoryManager memoryManager;
  private final ShuffleMemoryManager shuffleMemoryManager;
  private final BlockManager blockManager;
  final TaskContext taskContext = TaskContext.get();

  /**
   * The buffer size to use when writing spills using DiskBlockObjectWriter
   */
  private final int fileBufferSizeBytes;

  /**
   * A linked list for tracking all allocated data pages so that we can free all of our memory.
   */
  private final List<MemoryBlock> dataPages = new LinkedList<MemoryBlock>();

  /**
   * The data page that will be used to store keys and values for new hashtable entries. When this
   * page becomes full, a new page will be allocated and this pointer will change to point to that
   * new page.
   */
  private MemoryBlock currentDataPage = null;

  /**
   * Offset into `currentDataPage` that points to the location where new data can be inserted into
   * the page. This does not incorporate the page's base offset.
   */
  private long pageCursor = 0;

  /**
   * The size of the data pages that hold key and value data. Map entries cannot span multiple
   * pages, so this limits the maximum entry size.
   */
  //private static final long PAGE_SIZE_BYTES = 1L << 26; // 64 megabytes

  private static final long PAGE_SIZE_BYTES = 1L << 20; // 64 megabytes

  @VisibleForTesting
  static final int DISK_WRITE_BUFFER_SIZE = 1024 * 1024;

  /**
   * An empty aggregation buffer, encoded in UnsafeRow format. When inserting a new key into the
   * map, we copy this buffer and use it as the value.
   */
  private byte[] emptyBuffer;

  /**
   * An empty row used by `initProjection`
   */
  private static final InternalRow emptyRow = new GenericInternalRow();

  /**
   * Whether can the empty aggregation buffer be reuse without calling `initProjection` or not.
   */
  private boolean reuseEmptyBuffer;

  /**
   * The projection used to initialize the emptyBuffer
   */
  private final Function1<InternalRow, InternalRow> initProjection;

  private MutableProjection updateProjection;
  private MutableProjection mergeProjection;

  /**
   * Encodes grouping keys or buffers as UnsafeRows.
   */
  private final UnsafeRowConverter keyConverter;
  private final UnsafeRowConverter bufferConverter;

  /**
   * A hashmap which maps from opaque bytearray keys to bytearray values.
   */
  private UnsafeAppendOnlyMap map;

  /**
   * An object pool for objects that are used in grouping keys.
   */
  private UniqueObjectPool keyPool;

  /**
   * An object pool for objects that are used in aggregation buffers.
   */
  private ObjectPool bufferPool;

  /**
   * Re-used pointer to the current aggregation buffer
   */
  private final UnsafeRow currentBuffer = new UnsafeRow();

  /**
   * Scratch space that is used when encoding grouping keys into UnsafeRow format.
   * <p/>
   * By default, this is a 8 kb array, but it will grow as necessary in case larger keys are
   * encountered.
   */
  private byte[] groupingKeyConversionScratchSpace = new byte[1024 * 8];

  private boolean enablePerfMetrics;

  private Ordering<InternalRow> groupingKeyOrdering;
  private int groupingKeyNum;

  private int testSpillFrequency = 0;

  private long numRowsInserted = 0;

  private final LinkedList<DiskMapIterator> spills =
      new LinkedList<DiskMapIterator>();

  private final SerializerInstance serializerInstance;

  /**
   * Create a new UnsafeFixedWidthAggregationMap.
   *
   * @param initProjection    the default value for new keys (a "zero" of the agg. function)
   * @param updateProjection
   * @param mergeProjection
   * @param keyConverter      the converter of the grouping key, used for row conversion.
   * @param bufferConverter   the converter of the aggregation buffer, used for row conversion.
   * @param groupingKeyOrdering
   * @param initialCapacity   the initial capacity of the map (a sizing hint to avoid re-hashing).
   * @param enablePerfMetrics if true, performance metrics will be recorded (has minor perf impact)
   */
  public UnsafeFixedWidthExternalAggregation(
      Function1<InternalRow, InternalRow> initProjection,
      MutableProjection updateProjection,
      MutableProjection mergeProjection,
      UnsafeRowConverter keyConverter,
      UnsafeRowConverter bufferConverter,
      Ordering<InternalRow> groupingKeyOrdering,
      Serializer serializer,
      int initialCapacity,
      boolean enablePerfMetrics) throws IOException {
    this.initProjection = initProjection;
    this.updateProjection = updateProjection;
    this.mergeProjection = mergeProjection;
    this.keyConverter = keyConverter;
    this.bufferConverter = bufferConverter;
    this.groupingKeyOrdering = groupingKeyOrdering;
    this.groupingKeyNum = keyConverter.numFields();
    this.serializerInstance = serializer.newInstance();
    this.initialCapacity = initialCapacity;
    this.enablePerfMetrics = enablePerfMetrics;

    this.memoryManager = TaskContext.get().taskMemoryManager();
    final SparkEnv sparkEnv = SparkEnv.get();
    this.shuffleMemoryManager = sparkEnv.shuffleMemoryManager();
    this.blockManager = sparkEnv.blockManager();

    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSizeBytes =
        (int) sparkEnv.conf().getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;

    initializeUnsafeAppendMap();
  }

  /**
   * Allocates new sort data structures. Called when creating the sorter and after each spill.
   */
  private void initializeUnsafeAppendMap() throws IOException {
    this.keyPool = new UniqueObjectPool(100);
    this.bufferPool = new ObjectPool(initialCapacity);

    InternalRow initRow = initProjection.apply(emptyRow);
    this.emptyBuffer = new byte[bufferConverter.getSizeRequirement(initRow)];
    int writtenLength = bufferConverter.writeRow(
        initRow, emptyBuffer, PlatformDependent.BYTE_ARRAY_OFFSET, bufferPool);
    assert (writtenLength == emptyBuffer.length) : "Size requirement calculation was wrong!";
    // re-use the empty buffer only when there is no object saved in pool.
    reuseEmptyBuffer = bufferPool.size() == 0;

    this.map = new UnsafeAppendOnlyMap(memoryManager, initialCapacity,
        enablePerfMetrics);
  }

  /**
   * Return true if it can external aggregate with the groupKey schema & aggregationBuffer schema,
   * false otherwise
   */
  public static boolean supportsSchema(StructType groupKeySchema,
      StructType aggregationBufferSchema) {
    for (StructField field : groupKeySchema.fields()) {
      if (UnsafeColumnWriter.forType(field.dataType()) instanceof ObjectUnsafeColumnWriter) {
        return false;
      }
    }
    for (StructField field : aggregationBufferSchema.fields()) {
      if (UnsafeColumnWriter.forType(field.dataType()) instanceof ObjectUnsafeColumnWriter) {
        return false;
      }
    }
    return true;
  }

  /**
   * Forces spills to occur every `frequency` records. Only for use in tests.
   */
  @VisibleForTesting
  public void setTestSpillFrequency(int frequency) {
    assert frequency > 0 : "Frequency must be positive";
    testSpillFrequency = frequency;
  }

  public void insertRow(InternalRow groupingKey, InternalRow currentRow)
      throws IOException {
    System.out.println("groupingKey==" + groupingKey);
    System.out.println("currentRow==" + currentRow);
    numRowsInserted += 1;
    int groupingKeySize = keyConverter.getSizeRequirement(groupingKey);
    // Make sure that the buffer is large enough to hold the key. If it's not, grow it:
    if (groupingKeySize > groupingKeyConversionScratchSpace.length) {
      groupingKeyConversionScratchSpace = new byte[groupingKeySize];
    }
    numRowsInserted++;
    if (testSpillFrequency > 0 && (numRowsInserted % testSpillFrequency) == 0) {
      spill();
    }
    UnsafeRow aggregationBuffer = this.getAggregationBuffer(groupingKey, groupingKeySize);
    JoinedRow3 joinedRow = new JoinedRow3(aggregationBuffer, currentRow);
    this.updateProjection.target(aggregationBuffer).apply(joinedRow);
  }

  /**
   * Return the aggregation buffer for the current group. For efficiency, all calls to this method
   * return the same object.
   */
  public UnsafeRow getAggregationBuffer(InternalRow groupingKey, int groupingKeySize)
      throws IOException {
    final int actualGroupingKeySize = keyConverter.writeRow(
        groupingKey,
        groupingKeyConversionScratchSpace,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        keyPool);
    assert (groupingKeySize
        == actualGroupingKeySize) : "Size requirement calculation was wrong!";

    Object groupingKeyBaseObject = groupingKeyConversionScratchSpace;
    // Probe our map using the serialized key
    final UnsafeAppendOnlyMap.Location loc = map.lookup(
        groupingKeyBaseObject,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        groupingKeySize);
    if (!loc.isDefined()) {
      // This is the first time that we've seen this grouping key, so we'll insert a copy of the
      // empty aggregation buffer into the map:
      if (!reuseEmptyBuffer) {
        // There is some objects referenced by emptyBuffer, so generate a new one
        InternalRow initRow = initProjection.apply(emptyRow);
        bufferConverter
            .writeRow(initRow, emptyBuffer, PlatformDependent.BYTE_ARRAY_OFFSET,
                bufferPool);
      }
      if ( !this.putNewKey(
          groupingKeyBaseObject,
          PlatformDependent.BYTE_ARRAY_OFFSET,
          groupingKeySize,
          emptyBuffer,
          PlatformDependent.BYTE_ARRAY_OFFSET,
          emptyBuffer.length,
          loc)) {
        // because spill makes putting new key failed, it should get AggregationBuffer again
        return this.getAggregationBuffer(groupingKey, groupingKeySize);
      }
    }
    // Reset the pointer to point to the value that we just stored or looked up:
    final MemoryLocation address = loc.getValueAddress();
    currentBuffer.pointTo(
        address.getBaseObject(),
        address.getBaseOffset(),
        bufferConverter.numFields(),
        bufferPool
    );
    return currentBuffer;
  }

  public boolean putNewKey(
      Object keyBaseObject,
      long keyBaseOffset,
      int keyLengthBytes,
      Object valueBaseObject,
      long valueBaseOffset,
      int valueLengthBytes,
      UnsafeAppendOnlyMap.Location location) throws IOException {
    assert (!location.isDefined()) : "Can only set value once for a key";

    assert (keyLengthBytes % 8 == 0);
    assert (valueLengthBytes % 8 == 0);

    // Here, we'll copy the data into our data pages. Because we only store a relative offset from
    // the key address instead of storing the absolute address of the value, the key and value
    // must be stored in the same memory page.
    // (8 byte key length) (key) (8 byte value length) (value)
    final long requiredSize = 8 + keyLengthBytes + 8 + valueLengthBytes;
    if (!allocateSpaceForRecord(requiredSize)){
      // if spill have been happened, re-insert current groupingKey
      return false;
    }

    // Compute all of our offsets up-front:
    final Object pageBaseObject = currentDataPage.getBaseObject();
    final long pageBaseOffset = currentDataPage.getBaseOffset();
    final long keySizeOffsetInPage = pageBaseOffset + pageCursor;
    pageCursor += 8; // word used to store the key size
    final long keyDataOffsetInPage = pageBaseOffset + pageCursor;
    pageCursor += keyLengthBytes;
    final long valueSizeOffsetInPage = pageBaseOffset + pageCursor;
    pageCursor += 8; // word used to store the value size
    final long valueDataOffsetInPage = pageBaseOffset + pageCursor;
    pageCursor += valueLengthBytes;

    // Copy the key
    PlatformDependent.UNSAFE
        .putLong(pageBaseObject, keySizeOffsetInPage, keyLengthBytes);
    PlatformDependent.copyMemory(
        keyBaseObject, keyBaseOffset, pageBaseObject, keyDataOffsetInPage,
        keyLengthBytes);
    // Copy the value
    PlatformDependent.UNSAFE
        .putLong(pageBaseObject, valueSizeOffsetInPage, valueLengthBytes);
    PlatformDependent.copyMemory(
        valueBaseObject, valueBaseOffset, pageBaseObject, valueDataOffsetInPage,
        valueLengthBytes);

    final long storedKeyAddress = memoryManager.encodePageNumberAndOffset(
        currentDataPage, keySizeOffsetInPage);
    //System.out.println("keyDataOffsetInPage=" + keyDataOffsetInPage +" valueDataOffsetInPage=" +valueDataOffsetInPage);
    //System.out.println("storedKeyAddress=" + storedKeyAddress);
    this.map.putNewKey(storedKeyAddress, location);
    return true;
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the {@link org.apache.spark.shuffle.ShuffleMemoryManager} and
   * spill if the requested memory can not be obtained.
   *
   * @param requiredSpace the required space in the data page, in bytes, including space for storing
   *                      the record size.
   */
  private boolean allocateSpaceForRecord(long requiredSpace) throws IOException {
    boolean noSpill = true;
    assert (requiredSpace <= PAGE_SIZE_BYTES - 8); // Reserve 8 bytes for the end-of-page marker.
    // If there's not enough space in the current page, allocate a new page (8 bytes are reserved
    // for the end-of-page marker).
    if (currentDataPage == null || PAGE_SIZE_BYTES - 8 - pageCursor < requiredSpace) {
      logger.trace("Required space {} is less than free space in current page ({})",
          requiredSpace, PAGE_SIZE_BYTES - 8 - pageCursor);
      if (currentDataPage != null) {
        // There wasn't enough space in the current page, so write an end-of-page marker:
        final Object pageBaseObject = currentDataPage.getBaseObject();
        final long lengthOffsetInPage = currentDataPage.getBaseOffset() + pageCursor;
        PlatformDependent.UNSAFE.putLong(pageBaseObject, lengthOffsetInPage, END_OF_PAGE_MARKER);
      }
      long memoryAcquired = shuffleMemoryManager.tryToAcquire(PAGE_SIZE_BYTES);
      if (memoryAcquired < PAGE_SIZE_BYTES) {
        shuffleMemoryManager.release(memoryAcquired);
        spill();
        noSpill = false;
        final long memoryAcquiredAfterSpilling = shuffleMemoryManager.tryToAcquire(PAGE_SIZE_BYTES);
        if (memoryAcquiredAfterSpilling != PAGE_SIZE_BYTES) {
          shuffleMemoryManager.release(memoryAcquiredAfterSpilling);
          throw new IOException(
              "Unable to acquire " + PAGE_SIZE_BYTES + " bytes of memory");
        } else {
          memoryAcquired = memoryAcquiredAfterSpilling;
        }
      }
      MemoryBlock newPage = memoryManager.allocatePage(memoryAcquired);
      System.out.println("memoryAcquired==" + memoryAcquired);
      dataPages.add(newPage);
      pageCursor = 0;
      currentDataPage = newPage;
    }
    return noSpill;
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   */
  @VisibleForTesting
  void spill() throws IOException {
    logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
        Thread.currentThread().getId(),
        Utils.bytesToString(getMemoryUsage()),
        spills.size(),
        spills.size() > 1 ? " times" : " time");

    writeSortedFile();
    final long sorterMemoryUsage = map.getMemoryUsage();
    map = null;
    //shuffleMemoryManager.release(sorterMemoryUsage);
    final long spillSize = freeMemory();
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);

    initializeUnsafeAppendMap();
  }

  private void writeSortedFile() throws IOException {
    ShuffleWriteMetrics writeMetricsToUse = new ShuffleWriteMetrics();
    // Currently, we need to open a new DiskBlockObjectWriter for each partition; we can avoid this
    // after SPARK-5581 is fixed.
    BlockObjectWriter writer;

    // Small writes to DiskBlockObjectWriter will be fairly inefficient. Since there doesn't seem to
    // be an API to directly transfer bytes from managed memory to the disk writer, we buffer
    // data through a byte array. This array does not need to be large enough to hold a single
    // record;
    final byte[] writeBuffer = new byte[DISK_WRITE_BUFFER_SIZE];

    // Because this output will be read during shuffle, its compression codec must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more details.
    final Tuple2<TempLocalBlockId, File> spilledFileInfo =
        blockManager.diskBlockManager().createTempLocalBlock();
    final File file = spilledFileInfo._2();
    final TempLocalBlockId blockId = spilledFileInfo._1();
    // Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
    // Our write path doesn't actually use this serializer (since we end up calling the `write()`
    // OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
    // around this, we pass a dummy no-op serializer.

    writer = blockManager.getDiskWriter(blockId, file, this.serializerInstance, fileBufferSizeBytes,
        writeMetricsToUse);
    Iterator<RowEntry> imMemoryIter = this.getSortedIterator();
    System.out.println("file=" + file.getAbsolutePath());
    while (imMemoryIter.hasNext()) {
      RowEntry entry = imMemoryIter.next();
      //todo create SerializerInstance
      //System.out.println("key=" + entry.key.getInt(0) + ",value=" + entry.value.getLong(0));
      writer.write(entry.key, entry.value);
      writer.recordWritten();
    }
    if (writer != null) {
      writer.commitAndClose();
      spills.add(new DiskMapIterator(file, blockId));
    }
  }

  public Iterator<RowEntry> getSortedIterator() {
    return new Iterator<RowEntry>() {

      Iterator<UnsafeAppendOnlyMap.Location> sorter =
          map.getSortedIterator(dataPages, groupingKeyOrdering, groupingKeyNum,
              keyPool);

      @Override
      public boolean hasNext() {
        return sorter.hasNext();
      }

      @Override
      public RowEntry next() {
        MapEntry entry = new MapEntry();
        final UnsafeAppendOnlyMap.Location loc = sorter.next();
        final MemoryLocation keyAddress = loc.getKeyAddress();
        final MemoryLocation valueAddress = loc.getValueAddress();
        entry.key.pointTo(
            keyAddress.getBaseObject(),
            keyAddress.getBaseOffset(),
            keyConverter.numFields(),
            keyPool
        );
        entry.value.pointTo(
            valueAddress.getBaseObject(),
            valueAddress.getBaseOffset(),
            bufferConverter.numFields(),
            bufferPool
        );

        //System.out.println("memory k==" + entry.key.get(0));
        //System.out.println("memory v==" + entry.value.get(0));
        return new RowEntry(entry.key, entry.value);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Returns an iterator over the keys and values in this map.
   * <p/>
   * For efficiency, each call returns the same object.
   */
  public Iterator<RowEntry> iterator() {
    if (spills.isEmpty()) {
      return this.getMemoryIterator();
    } else {
      return this.merge(this.getSortedIterator());
    }
  }

  /**
   * An iterator that returns (K, C) pairs in sorted order from an on-disk map
   */
  public class DiskMapIterator implements Iterator<RowEntry> {

    private final ClassTag<Object> OBJECT_CLASS_TAG =
        ClassTag$.MODULE$.Object();
    private FileInputStream fileStream = null;

    // An intermediate stream that reads from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    private DeserializationStream deserializeStream;
    private RowEntry nextItem = null;

    private File file;
    private BlockId blockId;

    public DiskMapIterator(File file, BlockId blockId) {
      this.file = file;
      this.blockId = blockId;
    }

    private DeserializationStream nextBatchStream()
        throws FileNotFoundException {
      fileStream = new FileInputStream(file);
      InputStream bufferedStream = new BufferedInputStream(fileStream);
      InputStream compressedStream =
          blockManager.wrapForCompression(blockId, bufferedStream);
      return serializerInstance.deserializeStream(compressedStream);
    }

    private RowEntry readNextItem() throws IOException {
      if (deserializeStream == null) {
        deserializeStream = this.nextBatchStream();
      }
      InternalRow k = (InternalRow) deserializeStream.readKey(OBJECT_CLASS_TAG);
      InternalRow v = (InternalRow) deserializeStream.readValue(OBJECT_CLASS_TAG);
      //System.out.println("disk k===" + k.get(0));
      //System.out.println("disk v===" + v.get(0));
      return new RowEntry(k.copy(), v.copy());

    }

    @Override
    public boolean hasNext() {
      if (nextItem == null) {
        try {
          nextItem = readNextItem();
        } catch (IOException ie) {
          return false;
        }
      }
      return nextItem != null;
    }

    @Override
    public RowEntry next() {
      if (nextItem == null) {
        try {
          nextItem = readNextItem();
        } catch (IOException ie) {
          throw new UnsupportedOperationException(ie.getMessage());
        }
      }
      RowEntry item = nextItem;
      nextItem = null;
      return item;
    }

    @Override
    public void remove() {}
  }

  public class BufferedIterator implements Iterator<RowEntry> {

    private RowEntry hd = null;
    private boolean hdDefined = false;
    private Iterator<RowEntry> iterator;

    public BufferedIterator(Iterator<RowEntry> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return this.hdDefined || iterator.hasNext();
    }

    public RowEntry head() {

      if (!hdDefined) {
        hd = next();
        hdDefined = true;
      }
      //System.out.println("head==" + hd.key.get(0));
      return hd;
    }

    @Override
    public RowEntry next() {
      if (hdDefined) {
        hdDefined = false;
        return hd;
      } else {
        return iterator.next();
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  public BufferedIterator asBuffered(Iterator<RowEntry> iterator) {
    return new BufferedIterator(iterator);
  }

  /**
   * Mutable pair object
   */
  public class MapEntry {

    public UnsafeRow key;
    public UnsafeRow value;

    public MapEntry() {
      this.key = new UnsafeRow();
      this.value = new UnsafeRow();
    }

    public MapEntry(UnsafeRow key, UnsafeRow value) {
      this.key = key;
      this.value = value;
    }
  }

  public class RowEntry {

    public InternalRow key;
    public InternalRow value;

    public RowEntry(InternalRow key, InternalRow value) {
      this.key = key;
      this.value = value;
    }
  }

  /**
   * Returns an iterator over the keys and values in this map.
   * <p/>
   * For efficiency, each call returns the same object.
   */
  public Iterator<RowEntry> getMemoryIterator() {
    return new Iterator<RowEntry>() {

      private final MapEntry entry = new MapEntry();
      private final Iterator<UnsafeAppendOnlyMap.Location> mapLocationIterator =
          map.iterator(dataPages);

      @Override
      public boolean hasNext() {
        return mapLocationIterator.hasNext();
      }

      @Override
      public RowEntry next() {
        final UnsafeAppendOnlyMap.Location loc = mapLocationIterator.next();
        final MemoryLocation keyAddress = loc.getKeyAddress();
        final MemoryLocation valueAddress = loc.getValueAddress();
        entry.key.pointTo(
            keyAddress.getBaseObject(),
            keyAddress.getBaseOffset(),
            keyConverter.numFields(),
            keyPool
        );
        entry.value.pointTo(
            valueAddress.getBaseObject(),
            valueAddress.getBaseOffset(),
            bufferConverter.numFields(),
            bufferPool
        );
        return new RowEntry(entry.key, entry.value);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private Iterator<RowEntry> merge(Iterator<RowEntry> inMemory) {

    final Comparator<BufferedIterator> ordering1 =
        new Comparator<BufferedIterator>() {
          public int compare(BufferedIterator o1, BufferedIterator o2) {
            //System.out.println("o1=" + o1.head().key.getInt(0));
            //System.out.println("o2=" + o2.head().key.getInt(0));
            int comp = groupingKeyOrdering.compare(o1.head().key, o2.head().key);
            //System.out.println("comp=" + comp);
            //comp = 1;
            return (comp < 0) ? -1 : (comp > 0) ? 1 : 0;
          }
        };
    final Queue<BufferedIterator> priorityQueue =
        new PriorityQueue<BufferedIterator>
            (spills.size() + 1, ordering1);
    BufferedIterator inMemoryBuffer = this.asBuffered(inMemory);
//    System.out.println("==print inMemory==");
//    while (inMemoryBuffer.hasNext()) {
//      RowEntry firstPair = inMemoryBuffer.next();
//      System.out.println("inMemory k==" + firstPair.key.getInt(0));
//      System.out.println("inMemory v==" + firstPair.value.getLong(0));
//    }
    if (inMemoryBuffer.hasNext()) {
      priorityQueue.add(inMemoryBuffer);
    }

    for (int i = 0; i < spills.size(); i++) {
      BufferedIterator spillBuffer = this.asBuffered(spills.get(i));

//      while (spillBuffer.hasNext()) {
//        RowEntry firstPair = spillBuffer.next();
//        System.out.println("disk:" + i +" k==" + firstPair.key.getInt(0));
//        System.out.println("disk:" + i +" v==" + firstPair.value.getLong(0));
//      }
      if (spillBuffer.hasNext()) {
        priorityQueue.add(spillBuffer);
      }
    }
    final Iterator<RowEntry> iterator = new Iterator<RowEntry>() {
      BufferedIterator topIter = null;

      @Override
      public boolean hasNext() {
        return !priorityQueue.isEmpty() || (topIter != null && topIter.hasNext());
      }

      @Override
      public RowEntry next() {
        if (topIter != null && topIter.hasNext()) {
          priorityQueue.add(topIter);
        }
        topIter = priorityQueue.poll();
        RowEntry firstPair = topIter.next();
        //System.out.println("Queue k==" + firstPair.key.getInt(0));
        //System.out.println("Queue v==" + firstPair.value.getLong(0));
        //System.out.println("priorityQueue k==" + firstPair.key.getInt(0));
        //System.out.println("priorityQueue v==" + firstPair.value.getLong(0));
        return firstPair;
      }

      @Override
      public void remove() {
      }
    };

    final BufferedIterator sorted = asBuffered(iterator);
//        System.out.println("==print sorted==");
//        while (sorted.hasNext()) {
//          RowEntry firstPair = sorted.next();
//          System.out.println("sorted k==" + firstPair.key.getInt(0));
//          System.out.println("sorted v==" + firstPair.value.getLong(0));
//        }
    return new Iterator<RowEntry>() {

      @Override
      public boolean hasNext() {
        return sorted.hasNext();
      }

      @Override
      public RowEntry next() {
        RowEntry firstPair = sorted.next();
        InternalRow key = firstPair.key;
        MutableRow value = firstPair.value.makeMutable();
//        System.out.println("result's key=" + key.getInt(0));
//        System.out.println("result's value=" + value.getLong(0));

//        long[] result = new long[value.length()];
//        for (int i = 0; i < value.length(); i++) {
//            result[i] = (long) value.getLong(i);
//        }
        while (sorted.hasNext()) {
          System.out.println("sorted's key=" + key.getInt(0));
          System.out.println("sorted's head=" + sorted.head().key.getInt(0));
          //int comp = 1;
          int comp = groupingKeyOrdering.compare(sorted.head().key, key);
          //System.out.println("sorted's comp=" + comp);
          if (comp != 0) {
            break;
          }
          RowEntry pair = sorted.next();
          InternalRow value2 = pair.value;

          System.out.println("sorted's value2=" + value2.getLong(0));
          JoinedRow3 joinedRow = new JoinedRow3(value, value2);
          mergeProjection.target(value).apply(joinedRow);
//          for (int i = 0; i < value.length(); i++) {
//            result[i] += (long) value2.getLong(i);
//          }
        }
//        MutableRow rValue = new GenericMutableRow(result.length);
//        for (int i = 0; i < value.length(); i++) {
//          rValue.setLong(i, result[i]);
//        }
        System.out.println("result's key=" + key.getInt(0));
        System.out.println("result's value=" + value.getLong(0));
        return new RowEntry(key, value);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private long getMemoryUsage() {
    return map.getMemoryUsage() + (dataPages.size() * (long) PAGE_SIZE_BYTES);
  }

  /**
   * Free the unsafe memory associated with this map.
   */
  public long freeMemory() {
    long memoryFreed = 0;
    for (MemoryBlock block : dataPages) {
      memoryManager.freePage(block);
      shuffleMemoryManager.release(block.size());
      memoryFreed += block.size();
    }
    if (map != null) {
      map.free();
    }
    dataPages.clear();
    currentDataPage = null;
    pageCursor = 0;
    return memoryFreed;
  }

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public void printPerfMetrics() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException("Perf metrics not enabled");
    }
    System.out.println("Average probes per lookup: " + map.getAverageProbesPerLookup());
    System.out.println("Number of hash collisions: " + map.getNumHashCollisions());
    System.out.println("Time spent resizing (ns): " + map.getTimeSpentResizingNs());
    System.out.println("Total memory consumption (bytes): " + map.getTotalMemoryConsumption());
    System.out.println("Number of unique objects in keys: " + keyPool.size());
    System.out.println("Number of objects in buffers: " + bufferPool.size());
  }

}
