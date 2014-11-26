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

package org.apache.spark.shuffle

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.storage._

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.StandaloneShuffleBlockManager#getSortBasedShuffleBlockData().
private[spark]
class IndexShuffleBlockManager(conf: SparkConf) extends ShuffleBlockManager {

  private lazy val blockManager = SparkEnv.get.blockManager

  private val transportConf = SparkTransportConf.fromSparkConf(conf)

  private val totalMemoryAllowed:Int = conf.getInt("spark.shuffle.sort.indexCache.capacity", 100)

  private val totalMemoryUsed:AtomicInteger = new AtomicInteger()

  private val cache = new ConcurrentHashMap[String, Array[Long]]()

  private val queue = new LinkedBlockingQueue[String]()

  /**
   * Mapping to a single shuffleBlockId with reduce ID 0.
   * */
  def consolidateId(shuffleId: Int, mapId: Int): ShuffleBlockId = {
    ShuffleBlockId(shuffleId, mapId, 0)
  }

  def getDataFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, 0))
  }

  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, 0))
  }

  private def freeIndexInformation(): Unit = synchronized {
    while (totalMemoryUsed.get() > totalMemoryAllowed) {
      val s: String = queue.remove()
      val info: Array[Long] = cache.remove(s)
      if (info != null) {
        totalMemoryUsed.addAndGet(-1)
      }
    }
  }

  /**
   * Remove data file and index file that contain the output data from one map.
   * */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      file.delete()
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      file.delete()
    }
  }

  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockLocation to figure out where each block
   * begins and ends.
   * */
  def writeIndexFile(shuffleId: Int, mapId: Int, lengths: Array[Long]) = {
    val indexFile = getIndexFile(shuffleId, mapId)
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)))
    val offsets = new Array[Long](lengths.length + 1)
    try {
      // We take in lengths of each block, need to convert it to offsets.
      var offset = 0L
      offsets(0) = offset
      out.writeLong(offset)
      var i:Int = 0
      while (i < lengths.length) {
        offset += lengths(i)
        offsets(i + 1) = offset
        out.writeLong(offset)
        i += 1
      }
    } finally {
      out.close()
    }

    cache.put(shuffleId + "_" + mapId, offsets)
    queue.add(shuffleId + "_" + mapId)
    if (totalMemoryUsed.addAndGet(1) > totalMemoryAllowed) {
      freeIndexInformation()
    }
  }

  override def getBytes(blockId: ShuffleBlockId): Option[ByteBuffer] = {
    Some(getBlockData(blockId).nioByteBuffer())
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    val offsets: Array[Long] = cache.get(blockId.shuffleId + "_" + blockId.mapId)
    var offset: Long = 0
    var nextOffset: Long = 0
    if (offsets != null) {
      offset = offsets(blockId.reduceId)
      nextOffset = offsets(blockId.reduceId + 1)
    } else {
      val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)

      val in = new DataInputStream(new FileInputStream(indexFile))
      try {
        ByteStreams.skipFully(in, blockId.reduceId * 8)
        offset = in.readLong()
        nextOffset = in.readLong()
      } finally {
        in.close()
      }
    }

    new FileSegmentManagedBuffer(
      transportConf,
      getDataFile(blockId.shuffleId, blockId.mapId),
      offset,
      nextOffset - offset)

  }

  override def stop() = {}
}
