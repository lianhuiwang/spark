package org.apache.spark.sort.datagen

import java.io.{FileOutputStream, BufferedOutputStream, File}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.nativeio.NativeIO
import org.apache.spark.sort.{Utils, NodeLocalRDDPartition, NodeLocalRDD}
import org.apache.spark.{TaskContext, Partition, SparkConf, SparkContext}


object SortDataGenerator {

  private[this] val numTasksOnExecutor = new AtomicInteger

  def main(args: Array[String]): Unit = {
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt
    val dirs = args(2).split(",").map(_ + s"/sort-${sizeInGB}g-$numParts").toSeq
    val replica = args(3).toInt

    val sc = new SparkContext(new SparkConf().setAppName(
      s"DataGeneratorJava - $sizeInGB GB - $numParts parts $replica replica - ${args(2)}"))

    val hdfs = dirs.head.startsWith("hdfs")
    if (hdfs) {
      assert(dirs.size == 1)
      genSortHdfs(sc, sizeInGB, numParts, dirs.head, replica)
    } else {
      genSort(sc, sizeInGB, numParts, dirs, replica)
    }

  }

  def genSortHdfs(sc: SparkContext, sizeInGB: Int, numParts: Int, dir: String, replica: Int) {
    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val conf = new org.apache.hadoop.conf.Configuration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val root = new Path(dir)
    if (fs.exists(root)) {
      fs.mkdirs(root)
    }

    val hosts = Utils.readSlaves()
    val output = new NodeLocalRDD[(String, Int, String, Unsigned16)](sc, numParts, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index % numParts
        val host = split.asInstanceOf[NodeLocalRDDPartition].node

        val iter = generatePartition(part, recordsPerPartition.toInt)
        val outputFile = s"$dir/part$part.dat"
        val tempFile = outputFile + s".${context.partitionId}.${context.attemptId}.tmp"

        val conf = new org.apache.hadoop.conf.Configuration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)

        val blockSize = (recordsPerPartition * 100 / 512 + 1) * 512
        val out = fs.create(
          new Path(tempFile),
          false,  // overwrite
          4 * 1024 * 1024,  // buffer size
          replica.toShort,  // replication
          blockSize  // blocksize
        )

        val sum = new Unsigned16
        val checksum = new Unsigned16
        val crc32 = new PureJavaCrc32()

        while (iter.hasNext) {
          val buf = iter.next()

          crc32.reset()
          crc32.update(buf, 0, buf.length)
          checksum.set(crc32.getValue)
          sum.add(checksum)

          out.write(buf)
        }
        out.close()

        fs.rename(new Path(tempFile), new Path(outputFile))

        Iterator((host, part, outputFile, sum))
      }
    }.collect()

    val sum = new Unsigned16
    output.foreach { case (host, part, outputFile, checksum) =>
      println(s"$part\t$host\t$outputFile\t${checksum.toString}")
      sum.add(checksum)
    }
    println("sum: " + sum)
  }

  def genSort(sc: SparkContext, sizeInGB: Int, numParts: Int, dirs: Seq[String], replica: Int) {
    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val hosts = Utils.readSlaves()
    val numTasks = numParts * replica

    val rand = new java.util.Random(17)

    val replicatedHosts = Array.tabulate[String](numTasks) { i =>
      hosts(rand.nextInt(hosts.length))
    }

//    val replicatedHosts = new Array[String](numTasks)
//    for (replicaIndex <- 0 until replica) {
//      for (i <- 0 until numParts) {
//        replicatedHosts(replicaIndex * numParts + i) = hosts((i + replicaIndex) % hosts.length)
//      }
//    }

    val output = new NodeLocalRDD[(String, Int, String, Unsigned16)](sc, numTasks, replicatedHosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index % numParts
        val host = split.asInstanceOf[NodeLocalRDDPartition].node
        val baseFolder = dirs(numTasksOnExecutor.getAndIncrement() % dirs.size)

        val iter = generatePartition(part, recordsPerPartition.toInt)

        //val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g-$numParts"
        if (!new File(baseFolder).exists()) {
          new File(baseFolder).mkdirs()
        }
        val outputFile = s"$baseFolder/part$part.dat"

        val fileOutputStream = new FileOutputStream(outputFile)
        val os = new BufferedOutputStream(fileOutputStream, 4 * 1024 * 1024)

        val sum = new Unsigned16
        val checksum = new Unsigned16
        val crc32 = new PureJavaCrc32()

        while (iter.hasNext) {
          val buf = iter.next()

          crc32.reset()
          crc32.update(buf, 0, buf.length)
          checksum.set(crc32.getValue)
          sum.add(checksum)

          os.write(buf)
        }

//        NativeIO.POSIX.getCacheManipulator.posixFadviseIfPossible(
//          outputFile,
//          fileOutputStream.getFD,
//          0,
//          recordsPerPartition * 100,
//          NativeIO.POSIX.POSIX_FADV_DONTNEED)

        {
          val startTime = System.currentTimeMillis()
          NativeIO.POSIX.syncFileRangeIfPossible(
            fileOutputStream.getFD,
            0,
            recordsPerPartition * 100,
            NativeIO.POSIX.SYNC_FILE_RANGE_WRITE)
          val timeTaken = System.currentTimeMillis() - startTime
          logInfo(s"fsync $outputFile took $timeTaken ms")
        }

        os.close()

        Iterator((host, part, outputFile, sum))
      }
    }.collect()

    val sum = new Unsigned16
    output.foreach { case (host, part, outputFile, checksum) =>
      println(s"$part\t$host\t$outputFile\t${checksum.toString}")
      sum.add(checksum)
    }

    val checksumFile = s"/root/sort-${sizeInGB}g-$numParts.output"
    println(s"checksum output: $checksumFile")
    val writer = new java.io.PrintWriter(new File(checksumFile))
    output.foreach {  case (host, part, outputFile, checkSum) =>
      writer.write(s"$part\t$host\t$outputFile\t${checkSum.toString}\n")
    }
    writer.write("sum: " + sum)
    writer.close()

    println("sum: " + sum)
  }  // end of genSort

  def generatePartition(part: Int, recordsPerPartition: Int): Iterator[Array[Byte]] = {
    val one = new Unsigned16(1)
    val firstRecordNumber = new Unsigned16(part.toLong * recordsPerPartition)

    new Iterator[Array[Byte]] {
      private[this] val buf = new Array[Byte](100)
      private[this] val rand = Random16.skipAhead(firstRecordNumber)
      private[this] val recordNumber = new Unsigned16(firstRecordNumber)

      private[this] var i = 0
      private[this] val recordsPerPartition0 = recordsPerPartition

      override def hasNext: Boolean = i < recordsPerPartition0
      override def next(): Array[Byte] = {
        Random16.nextRand(rand)
        generateRecord(buf, rand, recordNumber)
        recordNumber.add(one)
        i += 1
        buf
      }
    }
  }

  /**
   * Generate a binary record suitable for all sort benchmarks except PennySort.
   *
   * @param recBuf record to return
   */
  private def generateRecord(recBuf: Array[Byte], rand: Unsigned16, recordNumber: Unsigned16): Unit = {
    // Generate the 10-byte key using the high 10 bytes of the 128-bit random number
    var i = 0
    while (i < 10) {
      recBuf(i) = rand.getByte(i)
      i += 1
    }

    // Add 2 bytes of "break"
    recBuf(10) = 0x00.toByte
    recBuf(11) = 0x11.toByte

    // Convert the 128-bit record number to 32 bits of ascii hexadecimal
    // as the next 32 bytes of the record.
    i = 0
    while (i < 32) {
      recBuf(12 + i) = recordNumber.getHexDigit(i).toByte
      i += 1
    }

    // Add 4 bytes of "break" data
    recBuf(44) = 0x88.toByte
    recBuf(45) = 0x99.toByte
    recBuf(46) = 0xAA.toByte
    recBuf(47) = 0xBB.toByte

    // Add 48 bytes of filler based on low 48 bits of random number
    i = 0
    while (i < 12) {
      val v = rand.getHexDigit(20 + i).toByte
      recBuf(48 + i * 4) = v
      recBuf(49 + i * 4) = v
      recBuf(50 + i * 4) = v
      recBuf(51 + i * 4) = v
      i += 1
    }

    // Add 4 bytes of "break" data
    recBuf(96) = 0xCC.toByte
    recBuf(97) = 0xDD.toByte
    recBuf(98) = 0xEE.toByte
    recBuf(99) = 0xFF.toByte
  }

}
