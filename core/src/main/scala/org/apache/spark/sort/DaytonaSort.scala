package org.apache.spark.sort

import java.io._
import java.util.concurrent.Semaphore

import io.netty.buffer.ByteBuf

import com.google.common.primitives.{Longs, UnsignedBytes}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, RemoteIterator, Path}

import org.apache.spark._
import org.apache.spark.sort.SortUtils._
import org.apache.spark.network.buffer.{ManagedBuffer, FileSegmentManagedBuffer, NettyManagedBuffer}
import org.apache.spark.rdd.{ShuffledRDD, RDD}

/**
* A version of the sort code that uses Unsafe to allocate off-heap blocks.
*/
object DaytonaSort extends Logging {

  /**
   * A semaphore to control concurrency when reading from disks. Right now we allow only eight
   * concurrent tasks to read. The rest will block.
   */
  private[this] val diskSemaphore = new Semaphore(8)

  private[this] val networkSemaphore = new Semaphore(8)

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("DaytonaSort [sizeInGB] [numParts] [replica] [input-dir]")
      System.exit(0)
    }

    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt
    val replica = args(2).toInt
    val dir = args(3)
    val outputDir = dir + "-out"

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100

    val sc = new SparkContext(new SparkConf().setAppName(
      s"DaytonaSort - $sizeInGB GB - $numParts parts $replica replica - $dir"))

    val conf = new org.apache.hadoop.conf.Configuration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val root = new Path(outputDir)
    if (fs.exists(root)) {
      fs.mkdirs(root)
    }

    val shuffled = createMapPartitions(sc, sizeInGB, numParts, dir, replica, pipeline = false)

    val recordsAfterSort: Long = shuffled.mapPartitionsWithContext { (context, iter) =>
      val part = context.partitionId
      val outputFile = s"$outputDir/part$part.dat"

      val startTime = System.currentTimeMillis()
      val sortBuffer = sortBuffers.get()
      assert(sortBuffer != null)
      var offset = 0L
      var numShuffleBlocks = 0

      sortBuffer.releaseMapSideBuffer()
      var offsetInChunk = 0L
      sortBuffer.allocateNewChunk()
      var totalBytesRead = 0L

      {
        logInfo(s"trying to acquire semaphore for $outputFile")
        val startTime = System.currentTimeMillis
        networkSemaphore.acquire()
        logInfo(s"acquired semaphore for $outputFile took " + (System.currentTimeMillis - startTime) + " ms")
      }

      while (iter.hasNext) {
        val n = iter.next()
        val a = n._2.asInstanceOf[ManagedBuffer]
        assert(a.size % 100 == 0, s"shuffle block size ${a.size} is wrong")

        //assert(a.size < sortBuffer.CHUNK_SIZE, s"buf len is ${a.size}")
        if (a.size > sortBuffer.CHUNK_SIZE) {
          println(s"buf size is ${a.size}")
        }
        if (offsetInChunk + a.size > sortBuffer.CHUNK_SIZE) {
          sortBuffer.markLastChunkUsage(offsetInChunk)
          sortBuffer.allocateNewChunk()
          offsetInChunk = 0
        }

        a match {
          case buf: NettyManagedBuffer =>
            val bytebuf = buf.convertToNetty().asInstanceOf[ByteBuf]
            val blockLen = bytebuf.readableBytes()
            assert(blockLen == a.size, s"len $blockLen a.size ${a.size}")
            if (blockLen > 0) {
              assert(blockLen % 100 == 0)
              assert(bytebuf.hasMemoryAddress)
              val start = bytebuf.memoryAddress + bytebuf.readerIndex

              var blockRead: Long = 0
              while (blockRead < blockLen) {
                val read0 = math.min(blockLen - blockRead, sortBuffer.CHUNK_SIZE - offsetInChunk)
                UNSAFE.copyMemory(start + blockRead, sortBuffer.currentChunkBaseAddress + offsetInChunk, read0)
                blockRead += read0
                offsetInChunk += read0
                if (offsetInChunk + (blockLen - blockRead) > sortBuffer.CHUNK_SIZE) {
                  sortBuffer.markLastChunkUsage(offsetInChunk)
                  sortBuffer.allocateNewChunk()
                  offsetInChunk = 0
                }
              }

              //offsetInChunk += len
              totalBytesRead += blockLen
            }
            bytebuf.release()

          case buf: FileSegmentManagedBuffer =>
            if (buf.size > 0) {
              val fs = new FileInputStream(buf.getFile)
              val channel = fs.getChannel
              channel.position(buf.getOffset)
              // Each shuffle block should not be bigger than our io buf capacity
              //assert(buf.length < sortBuffer.ioBuf.capacity,
              //  s"buf length is ${buf.length}} while capacity is ${sortBuffer.ioBuf.capacity}")
              var read = 0L
              while (read < buf.size()) {
                sortBuffer.ioBuf.clear()
                sortBuffer.ioBuf.limit(math.min(buf.size() - read, sortBuffer.IO_BUF_LEN).toInt)
                sortBuffer.setIoBufAddress(sortBuffer.currentChunkBaseAddress + offsetInChunk + read)
                val read0 = channel.read(sortBuffer.ioBuf)
                read += read0
              }
              assert(read == buf.size, s"read $read while size is ${buf.size} $buf")
              offsetInChunk += read
              totalBytesRead += read
              channel.close()
              fs.close()

//              val fs = new FileInputStream(buf.file)
//              val skipped = fs.skip(buf.offset)
//              assert(skipped == buf.offset, s"supposed to skip ${buf.offset} but got $skipped")
//              val bfs = new BufferedInputStream(fs, 128 * 1024)
//              val buf100 = new Array[Byte](100)
//              var read = 0L
//              while (read < buf.length) {
//                val read0 = bfs.read(buf100)
//                assert(read0 > 0, s"read0 is $read0")
//                UNSAFE.copyMemory(buf100, BYTE_ARRAY_BASE_OFFSET,
//                  null, sortBuffer.currentChunkBaseAddress + offsetInChunk + read,
//                  read0)
//                read += read0
//              }
//              assert(read == buf.length, s"read $read while size is ${buf.length} $buf")
//              offsetInChunk += read
//              totalBytesRead += read
//              bfs.close()
            }
        }

        numShuffleBlocks += 1
      }
      networkSemaphore.release()

      sortBuffer.markLastChunkUsage(offsetInChunk)

      val timeTaken = System.currentTimeMillis() - startTime
      logInfo(s"XXX Reduce: $timeTaken ms to fetch $numShuffleBlocks shuffle blocks ($totalBytesRead bytes) $outputFile")
      println(s"XXX Reduce: $timeTaken ms to fetch $numShuffleBlocks shuffle blocks ($totalBytesRead bytes) $outputFile")

      val numRecords = (totalBytesRead / 100).toInt

      // Sort!!!
      {
        val startTime = System.currentTimeMillis
        sortWithKeysUsingChunks(sortBuffer, numRecords)
        val timeTaken = System.currentTimeMillis - startTime
        logInfo(s"XXX Reduce: Sorting $numRecords records took $timeTaken ms $outputFile")
        println(s"XXX Reduce: Sorting $numRecords records took $timeTaken ms $outputFile")
      }

      val keys = sortBuffer.keys

      val count: Long = {
        val startTime = System.currentTimeMillis

        logInfo(s"XXX Reduce: writing $numRecords records started $outputFile")
        println(s"XXX Reduce: writing $numRecords records started $outputFile")
        val conf = new org.apache.hadoop.conf.Configuration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)

        val tempFile = outputFile + s".${context.partitionId}.${context.attemptId}.tmp"

        val os = fs.create(new Path(tempFile), replica.toShort)
        val buf = new Array[Byte](100)
        val arrOffset = BYTE_ARRAY_BASE_OFFSET
        val MASK = ((1 << 23) - 1).toLong // mask to get the lowest 23 bits
        var i = 0
        while (i < numRecords) {
          val locationInfo = keys(i * 2 + 1) & 0xFFFFFFFFL
          val chunkIndex = locationInfo >>> 23
          val indexWithinChunk = locationInfo & MASK
          UNSAFE.copyMemory(
            null,
            sortBuffer.chunkBegin(chunkIndex.toInt) + indexWithinChunk * 100,
            buf,
            arrOffset,
            100)
          os.write(buf)
          i += 1
        }
        os.close()
        fs.rename(new Path(tempFile), new Path(outputFile))

        sortBuffer.freeChunks()

        val timeTaken = System.currentTimeMillis - startTime
        logInfo(s"XXX Reduce: writing $numRecords records took $timeTaken ms $outputFile")
        println(s"XXX Reduce: writing $numRecords records took $timeTaken ms $outputFile")
        i.toLong
      }
      Iterator(count)
    }.reduce(_ + _)

    println("total number of records: " + recordsAfterSort)
  }

  def readFileIntoBuffer(inputFile: String, fileSize: Long, sortBuffer: SortBuffer) {
    logInfo(s"XXX start reading file $inputFile")
    println(s"XXX start reading file $inputFile with size $fileSize")
    val startTime = System.currentTimeMillis()
    assert(fileSize % 100 == 0)

    val conf = new Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val path = new Path(inputFile)
    var is: InputStream = null

    val baseAddress: Long = sortBuffer.address
    val buf = new Array[Byte](4 * 1024 * 1024)
    var read = 0L
    try {
      is = fs.open(path, 4 * 1024 * 1024)
      while (read < fileSize) {
        val read0 = is.read(buf)
        assert(read0 > 0, s"only read $read0 bytes this time; read $read; total $fileSize")
        UNSAFE.copyMemory(buf, BYTE_ARRAY_BASE_OFFSET, null, baseAddress + read, read0)
        read += read0
      }
      assert(read == fileSize)
    } finally {
      if (is != null) {
        is.close()
      }
    }
    val timeTaken = System.currentTimeMillis() - startTime
    logInfo(s"XXX finished reading file $inputFile ($read bytes), took $timeTaken ms")
    println(s"XXX finished reading file $inputFile ($read bytes), took $timeTaken ms")
    assert(read == fileSize)
  }

  def createMapPartitions(
      sc: SparkContext,
      sizeInGB: Int,
      numParts: Int,
      dir: String,
      replica: Int,
      pipeline: Boolean)
  : RDD[(Long, Array[Long])] = {

    val conf = new Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val path = new Path(dir)
    val statuses: RemoteIterator[LocatedFileStatus] = fs.listLocatedStatus(path)

    val replicatedHosts = new Array[Seq[String]](numParts)
    val startTime = System.currentTimeMillis()
    var i = 0
    while (statuses.hasNext) {
      val status = statuses.next()
      val filename = status.getPath.toString
      val blocks = status.getBlockLocations
      assert(blocks.size == 1, s"found blocks for $filename: " + blocks.toSeq)

      val partName = "part(\\d+).dat".r.findFirstIn(status.getPath.getName).get
      val part = partName.replace("part", "").replace(".dat", "").toInt
      replicatedHosts(part) = blocks.head.getHosts.toSeq
      i += 1
    }
    assert(i == numParts, "total file found: " + i)

    //replicatedHosts.zipWithIndex.foreach { case (a, i) => println(s"$i: $a") }

    val timeTaken = System.currentTimeMillis() - startTime
    logInfo(s"XXX took $timeTaken ms to get file metadata")
    println(s"XXX took $timeTaken ms to get file metadata")

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val totalRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(totalRecords.toDouble / numParts).toLong

    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////
    // Sample
    val rangeBounds = new Array[Long]((numParts - 1) * 2)

    {
      val startTime = System.currentTimeMillis()
      val samplePerPartition = new SparkConf().getInt("spark.samplePerPartition", 79)
      val numSampleKeys = numParts * samplePerPartition
      val sampleKeys = new NodeLocalReplicaRDD[Array[Byte]](sc, numParts, replicatedHosts) {
        override def compute(split: Partition, context: TaskContext) = {
          val part = split.index
          val inputFile = s"$dir/part$part.dat"

          val conf = new Configuration()
          val fs = org.apache.hadoop.fs.FileSystem.get(conf)
          val path = new Path(inputFile)
          val is = fs.open(path, 10)

          val skip = recordsPerPartition / samplePerPartition * 100

          val rand = new java.util.Random(part)
          val sampleLocs = Array.fill[Long](samplePerPartition)(
            math.abs(rand.nextLong()) % recordsPerPartition)
          java.util.Arrays.sort(sampleLocs)

          val samples = new Array[Array[Byte]](samplePerPartition)
          var sampleCount = 0
          while (sampleCount < samplePerPartition) {
            //is.seek(sampleCount * skip)
            is.seek(sampleLocs(sampleCount) * 100)
            // Read the first 10 byte, and save that.
            val buf = new Array[Byte](10)
            var read0 = is.read(buf)
            if (read0 < 10) {
              read0 += is.read(buf, read0, 10 - read0)
            }
            assert(read0 == 10, s"read $read0 bytes instead of 10 bytes, " +
              s"sampleCount $sampleCount, skip $skip")
            samples(sampleCount) = buf
            sampleCount += 1
          }

          samples.iterator
        }
      }.collect()

      val timeTaken = System.currentTimeMillis() - startTime
      logInfo(s"XXXX sampling ${sampleKeys.size} keys took $timeTaken ms")
      println(s"XXXX sampling ${sampleKeys.size} keys took $timeTaken ms")

      assert(sampleKeys.length == samplePerPartition * numParts,
        s"expect sampledKeys to be ${samplePerPartition * numParts}, but got ${sampleKeys.size}")

      java.util.Arrays.sort(sampleKeys, UnsignedBytes.lexicographicalComparator())

      var i = 0
      while (i < numParts - 1) {
        val k = sampleKeys((i + 1) * samplePerPartition)
        rangeBounds(i * 2) = Longs.fromBytes(0, k(0), k(1), k(2), k(3), k(4), k(5), k(6))
        rangeBounds(i * 2 + 1) = Longs.fromBytes(0, k(7), k(8), k(9), 0, 0, 0, 0)

        //println(s"range bound $i : ${k.toSeq.map(x => if (x<0) 256 + x else x)}")
//        if ( i > 0) {
//          println(s"range $i: ${rangeBounds(i * 2) - rangeBounds(i * 2 - 2)}")
//        } else {
//          println(s"range $i: ${rangeBounds(i * 2)}")
//        }
        i += 1
      }
    }

    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////
    val inputRdd = new NodeLocalReplicaRDD[(Long, Array[Long])](sc, numParts, replicatedHosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index

        val inputFile = s"$dir/part$part.dat"
        val fileSize = recordsPerPartition * 100

        if (sortBuffers.get == null) {
          val capacity = recordsPerPartition
          sortBuffers.set(new SortBuffer(capacity))
        }

        val sortBuffer = sortBuffers.get()

        {
          logInfo(s"trying to acquire semaphore for $inputFile")
          val startTime = System.currentTimeMillis
          diskSemaphore.acquire()
          logInfo(s"acquired semaphore for $inputFile took " + (System.currentTimeMillis - startTime) + " ms")
        }

        readFileIntoBuffer(inputFile, fileSize, sortBuffer)
        diskSemaphore.release()

        // Sort!!!
        {
          val startTime = System.currentTimeMillis
          sortWithKeys(sortBuffer, recordsPerPartition.toInt)
          val timeTaken = System.currentTimeMillis - startTime
          logInfo(s"XXX Sorting $recordsPerPartition records took $timeTaken ms")
          println(s"XXX Sorting $recordsPerPartition records took $timeTaken ms")
        }

        Iterator((recordsPerPartition, sortBuffer.keys))
      }
    }

    val partitioner = new DaytonaPartitioner(rangeBounds)
    new ShuffledRDD(inputRdd, partitioner)
      .setSerializer(new UnsafeSerializer(recordsPerPartition))
  }
}
