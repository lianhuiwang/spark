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

package org.apache.spark.shuffle.sort

import java.io.{FileOutputStream, BufferedOutputStream, File}

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.{LargeMapStatus, MapStatus}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.sort.SortUtils
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter

private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: SortShuffleFileWriter[K, V] = null

  private val isTestSort: Boolean = SparkEnv.get.conf.getBoolean("spark.test.sort", false)
  private val fileBufferSize =
    SparkEnv.get.conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = new ShuffleWriteMetrics()
  context.taskMetrics.shuffleWriteMetrics = Some(writeMetrics)

  def writeForTestSort(records: Iterator[Product2[K, V]]): Unit = {
    val (numRecords, pointers) = records.next().asInstanceOf[(Long, Array[Long])]

    val outputFile: File = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    //val blockId = shuffleBlockManager.consolidateId(dep.shuffleId, mapId)

    // Write the sorted output out.
    val ser = Serializer.getSerializer(dep.serializer)
    // Track location of each range in the output file
    val partitionLengths = new Array[Long](dep.partitioner.numPartitions)

    //SortShuffleWriter.sem.acquire()
    val startTime = System.currentTimeMillis()

    val UNSAFE = SortUtils.UNSAFE
    val BYTE_ARRAY_BASE_OFFSET = SortUtils.BYTE_ARRAY_BASE_OFFSET
    var i = 0
    var prevPid = 0
    var offsetWithinPartition = 0L
    var totalWritten = 0L
    //var writer: BlockObjectWriter = null
    //val pair = new MutablePair[Long, Long]

    val baseAddress = SortUtils.sortBuffers.get().address
    val out = new BufferedOutputStream(new FileOutputStream(outputFile), fileBufferSize)
    val buf = new Array[Byte](100)

    val codec = new org.apache.spark.io.LZFCompressionCodec(new SparkConf)

    dep.partitioner match {
      case p: org.apache.spark.sort.DaytonaPartitioner =>
        assert(numRecords * 2 <= pointers.length)
        p.setKeys()

        while (i < numRecords) {
          val pid = p.getPartitionSpecialized(pointers(i * 2), pointers(i * 2 + 1))
          if (pid != prevPid) {
            // This is a new pid. update the index.
            partitionLengths(prevPid) = offsetWithinPartition
            writeMetrics.incShuffleBytesWritten(offsetWithinPartition)
            offsetWithinPartition = 0L
            prevPid = pid
          }
          val addr = baseAddress + (pointers(i * 2 + 1) & 0xFFFFFFFFL) * 100
          UNSAFE.copyMemory(null, addr, buf, BYTE_ARRAY_BASE_OFFSET, 100)
          out.write(buf)
          offsetWithinPartition += 100
          totalWritten += 100
          i += 1
        }

      case p: org.apache.spark.sort.DaytonaPartitionerSkew =>
        assert(numRecords * 2 <= pointers.length)
        p.setKeys()

        while (i < numRecords) {
          val pid = p.getPartitionSpecialized(pointers(i * 2), pointers(i * 2 + 1))
          if (pid != prevPid) {

            if (pid > prevPid + 1) {
              val sizePerBucket = (offsetWithinPartition / (pid - prevPid)) / 100 * 100
              var left = offsetWithinPartition
              var j = prevPid
              while (j < pid - 1) {
                partitionLengths(j) = sizePerBucket
                left -= sizePerBucket
                j += 1
              }
              partitionLengths(j) = left
            } else {
              // This is a new pid. update the index.
              partitionLengths(prevPid) = offsetWithinPartition
            }
            writeMetrics.incShuffleBytesWritten(offsetWithinPartition)
            offsetWithinPartition = 0L
            prevPid = pid
          }
          val addr = baseAddress + (pointers(i * 2 + 1) & 0xFFFFFFFFL) * 100
          UNSAFE.copyMemory(null, addr, buf, BYTE_ARRAY_BASE_OFFSET, 100)
          out.write(buf)
          offsetWithinPartition += 100
          totalWritten += 100
          i += 1
        }

      case p: org.apache.spark.sort.IndyPartitioner =>
        assert(numRecords <= pointers.length)

        while (i < numRecords) {
          val pid = p.getPartitionSpecialized(pointers(i))
          if (pid != prevPid) {
            // This is a new pid. update the index.
            partitionLengths(prevPid) = offsetWithinPartition
            writeMetrics.incShuffleBytesWritten(offsetWithinPartition)
            offsetWithinPartition = 0L
            prevPid = pid
          }

          val addr = pointers(i)
          UNSAFE.copyMemory(null, addr, buf, BYTE_ARRAY_BASE_OFFSET, 100)
          out.write(buf)
          offsetWithinPartition += 100
          totalWritten += 100
          i += 1
        }

      case p: org.apache.spark.sort.IndyPartitionerPB =>
        assert(numRecords <= pointers.length)

        var compressedOut = codec.compressedOutputStream(out)
        var lastFileLen = 0L

        while (i < numRecords) {
          val pid = p.getPartitionSpecialized(pointers(i))
          if (pid != prevPid) {
            // This is a new pid. update the index.
            compressedOut.flush()
            compressedOut.close()

            val currentFileLen = outputFile.length()
            partitionLengths(prevPid) = currentFileLen - lastFileLen
            lastFileLen = currentFileLen
            writeMetrics.incShuffleBytesWritten(partitionLengths(prevPid))
            prevPid = pid

            val out1 = new BufferedOutputStream(new FileOutputStream(outputFile, true), fileBufferSize)
            compressedOut = codec.compressedOutputStream(out1)
          }

          val addr = pointers(i)
          UNSAFE.copyMemory(null, addr, buf, BYTE_ARRAY_BASE_OFFSET, 100)
          compressedOut.write(buf)
          totalWritten += 100
          i += 1
        }

        compressedOut.flush()
        compressedOut.close()
        val currentFileLen = outputFile.length()
        partitionLengths(prevPid) = currentFileLen - lastFileLen
        writeMetrics.incShuffleBytesWritten(partitionLengths(prevPid))

      case _ =>
        throw new RuntimeException("Unknown partitioner type " + dep.partitioner)
    }

    if (!dep.partitioner.isInstanceOf[org.apache.spark.sort.IndyPartitionerPB]) {
      // Handle the last partition
      out.flush()
      out.close()

      //SortShuffleWriter.sem.release()
      writeMetrics.incShuffleBytesWritten(offsetWithinPartition)


      dep.partitioner match {
        case p: org.apache.spark.sort.DaytonaPartitionerSkew =>

          var left = offsetWithinPartition
          val sizePerBucket = (offsetWithinPartition / (dep.partitioner.numPartitions - prevPid)) / 100 * 100
          var j = prevPid
          println(s"offsetWithinPartition $offsetWithinPartition prevPid $prevPid num parts ${dep.partitioner.numPartitions}")
          while (j < dep.partitioner.numPartitions - 1) {
            partitionLengths(j) = sizePerBucket
            left -= sizePerBucket
            j += 1
          }
          partitionLengths(j) = left

          println("index is " + partitionLengths.toSeq)
          println("total bucket len is " + partitionLengths.sum)

        case _ =>
          partitionLengths(prevPid) = offsetWithinPartition

          if (prevPid < dep.partitioner.numPartitions - 1) {
            var i = prevPid
            while (i < dep.partitioner.numPartitions - 1) {
              partitionLengths(i) = 0
              i += 1
            }
          }
      }
    }

    shuffleBlockResolver.writeIndexFile(dep.shuffleId, mapId, partitionLengths)

    val timeTaken = System.currentTimeMillis() - startTime
    logInfo("XXX Time taken to write shuffle files " + outputFile + ": " + timeTaken)
    println("XXX Time taken to write shuffle files " + outputFile + ": " + timeTaken)

    val averageBlockSize: Long = partitionLengths.sum / partitionLengths.length
    mapStatus = new LargeMapStatus(blockManager.blockManagerId, averageBlockSize)
    //  partitionLengths.map(MapOutputTracker.compressSize))
  }

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    if (isTestSort) {
      writeForTestSort(records)
    } else {
      sorter = if (dep.mapSideCombine) {
        require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
        new ExternalSorter[K, V, C](
          dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
      } else if (SortShuffleWriter.shouldBypassMergeSort(
        SparkEnv.get.conf, dep.partitioner.numPartitions, aggregator = None, keyOrdering = None)) {
        // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
        // need local aggregation and sorting, write numPartitions files directly and just concatenate
        // them at the end. This avoids doing serialization and deserialization twice to merge
        // together the spilled files, which would happen with the normal code path. The downside is
        // having multiple files open at a time and thus more memory allocated to buffers.
        new BypassMergeSortShuffleWriter[K, V](SparkEnv.get.conf, blockManager, dep.partitioner,
          writeMetrics, Serializer.getSerializer(dep.serializer))
      } else {
        // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
        // care whether the keys get sorted in each partition; that will be done on the reduce side
        // if the operation being run is sortByKey.
        new ExternalSorter[K, V, V](
          aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
      }
      sorter.insertAll(records)

      // Don't bother including the time to open the merged output file in the shuffle write time,
      // because it just opens a single file, so is typically too fast to measure accurately
      // (see SPARK-3570).
      val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
      val tmp = Utils.tempFileWith(output)
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      val partitionLengths = sorter.writePartitionedFile(blockId, context, tmp)
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        // The map task failed, so delete our output data.
        shuffleBlockResolver.removeDataByMap(dep.shuffleId, mapId)
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        context.taskMetrics.shuffleWriteMetrics.foreach(
          _.incShuffleWriteTime(System.nanoTime - startTime))
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {
  def shouldBypassMergeSort(
      conf: SparkConf,
      numPartitions: Int,
      aggregator: Option[Aggregator[_, _, _]],
      keyOrdering: Option[Ordering[_]]): Boolean = {
    val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    numPartitions <= bypassMergeThreshold && aggregator.isEmpty && keyOrdering.isEmpty
  }
}
