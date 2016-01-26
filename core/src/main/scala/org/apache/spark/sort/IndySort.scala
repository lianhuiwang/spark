package org.apache.spark.sort

import java.io._
import java.nio.channels.FileChannel
import java.util.concurrent.Semaphore

import scala.concurrent.future
import scala.concurrent.ExecutionContext.Implicits.global

import io.netty.buffer.ByteBuf

import org.apache.hadoop.io.nativeio.NativeIO

import org.apache.spark._
import org.apache.spark.sort.SortUtils._
import org.apache.spark.network.buffer.{ManagedBuffer, FileSegmentManagedBuffer, NettyManagedBuffer}
import org.apache.spark.rdd.{ShuffledRDD, RDD}

/**
 * A version of the sort code that uses Unsafe to allocate off-heap blocks.
 */
object IndySort extends Logging {

  /**
   * A semaphore to control concurrency when reading from disks. Right now we allow only eight
   * concurrent tasks to read. The rest will block.
   */
  private[this] val diskSemaphore = new Semaphore(8)
  private[this] val networkSemaphore = new Semaphore(16)

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("IndySort [sizeInGB] [numParts] [replica] [input-dir]")
      System.exit(0)
    }

    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt
    val replica = args(2).toInt
    val dir = args(3)

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val sc = new SparkContext(new SparkConf().setAppName(
      s"IndySort - $sizeInGB GB - $numParts parts $replica replica - $dir"))
    val input = createMapPartitions(sc, sizeInGB, numParts, dir, replica)

    val partitioner = new IndyPartitioner(numParts)
    val shuffled = new ShuffledRDD(input, partitioner)
      .setSerializer(new UnsafeSerializer(recordsPerPartition))

    val recordsAfterSort: Long = shuffled.mapPartitionsWithIndex { (part, iter) =>
      val baseFolder = dir + "-out"
      val outputFile = s"$baseFolder/part$part.dat"

      val startTime = System.currentTimeMillis()
      val sortBuffer = sortBuffers.get()
      assert(sortBuffer != null)
      var offset = 0L
      var numShuffleBlocks = 0

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
        a match {
          case buf: NettyManagedBuffer =>
            val bytebuf = buf.convertToNetty().asInstanceOf[ByteBuf]
            val len = bytebuf.readableBytes()
            assert(len % 100 == 0)
            assert(bytebuf.hasMemoryAddress)

            val start = bytebuf.memoryAddress + bytebuf.readerIndex
            UNSAFE.copyMemory(start, sortBuffer.address + offset, len)
            offset += len
            bytebuf.release()

//          case buf: FileSegmentManagedBuffer =>
//            val fs = new FileInputStream(buf.file)
//            val channel = fs.getChannel
//            channel.position(buf.offset)
//            // Each shuffle block should not be bigger than our io buf capacity
//            assert(buf.length < sortBuffer.ioBuf.capacity,
//              s"buf length is ${buf.length}} while capacity is ${sortBuffer.ioBuf.capacity}")
//            sortBuffer.ioBuf.clear()
//            sortBuffer.ioBuf.limit(buf.length.toInt)
//            sortBuffer.setIoBufAddress(sortBuffer.address + offset)
//            val read0 = channel.read(sortBuffer.ioBuf)
//            assert(read0 == buf.length)
//            offset += read0
//            channel.close()
//            fs.close()
        }

        numShuffleBlocks += 1
      }

      networkSemaphore.release()

      val timeTaken = System.currentTimeMillis() - startTime
      logInfo(s"XXX Reduce: $timeTaken ms to fetch $numShuffleBlocks shuffle blocks ($offset bytes) $outputFile")
      println(s"XXX Reduce: $timeTaken ms to fetch $numShuffleBlocks shuffle blocks ($offset bytes) $outputFile")

      val pointers = sortBuffer.pointers
      val numRecords = (offset / 100).toInt

        // Sort!!!
      {
        val startTime = System.currentTimeMillis
        sortWithKeys(sortBuffer, numRecords)
        val timeTaken = System.currentTimeMillis - startTime
        logInfo(s"XXX Reduce: Sorting $numRecords records took $timeTaken ms $outputFile")
        println(s"XXX Reduce: Sorting $numRecords records took $timeTaken ms $outputFile")
        scala.Console.flush()
      }

      val count: Long = {
        val startTime = System.currentTimeMillis
        if (!new File(baseFolder).exists()) {
          new File(baseFolder).mkdirs()
        }
        logInfo(s"XXX Reduce: writing $numRecords records started $outputFile")
        println(s"XXX Reduce: writing $numRecords records started $outputFile")

        //writeSemaphore.acquire()

        val fout = new FileOutputStream(outputFile)
        val fd = fout.getFD
        val os = new BufferedOutputStream(fout, 4 * 1024 * 1024)
        val buf = new Array[Byte](100)
        val arrOffset = BYTE_ARRAY_BASE_OFFSET
        var i = 0
        while (i < numRecords) {
          val addr = pointers(i)
          UNSAFE.copyMemory(null, addr, buf, arrOffset, 100)
          os.write(buf)
          i += 1
        }

        os.close()
        val timeTaken = System.currentTimeMillis - startTime
        logInfo(s"XXX Reduce: writing $numRecords records took $timeTaken ms $outputFile")
        println(s"XXX Reduce: writing $numRecords records took $timeTaken ms $outputFile")

        //writeSemaphore.release()

        i.toLong
      }
      Iterator(count)
    }.reduce(_ + _)

    println("total number of records: " + recordsAfterSort)
  }

  def readFileIntoBuffer(inputFile: String, sortBuffer: SortBuffer) {
    logInfo(s"XXX start reading file $inputFile")
    println(s"XXX start reading file $inputFile")
    val startTime = System.currentTimeMillis()
    val fileSize = new File(inputFile).length
    assert(fileSize % 100 == 0)

    val baseAddress: Long = sortBuffer.address
    var is: FileInputStream = null
    var channel: FileChannel = null
    var read = 0L
    var fd: FileDescriptor = null
    try {
      is = new FileInputStream(inputFile)
      fd = is.getFD
      channel = is.getChannel()
      while (read < fileSize) {
        // This should read read0 bytes directly into our buffer
        sortBuffer.ioBuf.clear()
        sortBuffer.setIoBufAddress(baseAddress + read)
        assert(
          sortBuffer.ioBufAddress >= baseAddress && sortBuffer.ioBufAddress <= baseAddress + fileSize,
          s"failed assertion $baseAddress <= ${sortBuffer.ioBufAddress} <= ${baseAddress + fileSize}")
        val read0 = channel.read(sortBuffer.ioBuf)
        read += read0
      }
    } finally {
      if (channel != null) {
        channel.close()
      }
      if (is != null) {
        is.close()
      }
    }

    if (fd != null) {
//      future {
//        // Drop from buffer cache
//        val f = new FileInputStream(inputFile)
//        NativeIO.POSIX.getCacheManipulator.posixFadviseIfPossible(
//          inputFile,
//          f.getFD,
//          0,
//          fileSize,
//          NativeIO.POSIX.POSIX_FADV_DONTNEED)
//        f.close()
//      }
    }

    val timeTaken = System.currentTimeMillis() - startTime
    logInfo(s"XXX finished reading file $inputFile ($read bytes), took $timeTaken ms")
    println(s"XXX finished reading file $inputFile ($read bytes), took $timeTaken ms")
    scala.Console.flush()
    assert(read == fileSize)
  }

  def createMapPartitions(
      sc: SparkContext,
      sizeInGB: Int,
      numParts: Int,
      dir: String,
      replica: Int): RDD[(Long, Array[Long])] = {

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val totalRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(totalRecords.toDouble / numParts).toLong

    val rand = new java.util.Random(17)

    val hosts = Utils.readSlaves()
//    val replicatedHosts = Array.tabulate[Seq[String]](hosts.length) { i =>
//      Seq.tabulate[String](replica) { replicaIndex =>
//        hosts((i + replicaIndex) % hosts.length)
//      }
//    }
    val replicatedHosts0 = Array.tabulate[String](numParts * replica) { i =>
      hosts(rand.nextInt(hosts.length))
    }

    val replicatedHosts = Array.tabulate[Seq[String]](numParts) { i =>
      Seq.tabulate[String](replica) { replicaIndex =>
        replicatedHosts0(i + replicaIndex * numParts)
      }
    }

    new NodeLocalReplicaRDD[(Long, Array[Long])](sc, numParts, replicatedHosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index

        val inputFile = dir + s"/part$part.dat"
        val fileSize = new File(inputFile).length
        assert(fileSize % 100 == 0)

        if (sortBuffers.get == null) {
          // Allocate 10% overhead since after shuffle the partitions can get slightly uneven.
          val capacity = recordsPerPartition + recordsPerPartition / 10
          sortBuffers.set(new SortBuffer(capacity))
        }

        val sortBuffer = sortBuffers.get()

        {
          logInfo(s"trying to acquire semaphore for $inputFile")
          val startTime = System.currentTimeMillis
          diskSemaphore.acquire()
          logInfo(s"acquired semaphore for $inputFile took " + (System.currentTimeMillis - startTime) + " ms")
        }

        readFileIntoBuffer(inputFile, sortBuffer)
        diskSemaphore.release()

        // Sort!!!
        {
          val startTime = System.currentTimeMillis
          sortWithKeys(sortBuffer, recordsPerPartition.toInt)
          val timeTaken = System.currentTimeMillis - startTime
          logInfo(s"XXX Sorting $recordsPerPartition records took $timeTaken ms")
          println(s"XXX Sorting $recordsPerPartition records took $timeTaken ms")
          scala.Console.flush()
        }

        Iterator((recordsPerPartition, sortBuffer.pointers))
      }
    }
  }
}
