package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{UnknownPartitioning, HashPartitioning}
import org.apache.spark.sql.types.{IntegerType, LongType, IntegralType}
import org.apache.spark.{ShuffleDependency, Logging, MapOutputStatistics}
import org.apache.spark.sql.execution.{ShuffledRowRDD, SparkPlan}

import scala.collection.mutable.{Queue, ArrayBuffer}

/**
 * Utility functions used by the query fragment.
 */
private[sql] object Utils extends Logging {

  private[sql] def findChildFragment(root: SparkPlan): Seq[QueryFragment] = {
    val result = new ArrayBuffer[QueryFragment]
    val queue = new Queue[SparkPlan]
    queue.enqueue(root)
    while (queue.nonEmpty) {
      val current = queue.dequeue()
      if (current.isInstanceOf[FragmentInput]) {
        val fragmentInput = current.asInstanceOf[FragmentInput]
        result += fragmentInput.childFragment
      } else {
        current.children.foreach(c => queue.enqueue(c))
      }
    }
    result
  }

  private[sql] def findLeafFragment(root: QueryFragment): Seq[QueryFragment] = {
    val result = new ArrayBuffer[QueryFragment]
    val queue = new Queue[QueryFragment]
    queue.enqueue(root)
    while (queue.nonEmpty) {
      val current = queue.dequeue()
      if (current.children.isEmpty) {
        result += current
      } else {
        current.children.foreach(c => queue.enqueue(c))
      }
    }
    result
  }

  /**
   * Estimates partition start indices for post-shuffle partitions based on
   * mapOutputStatistics provided by all pre-shuffle stages.
   */
  private[sql] def estimatePartitionStartIndices(
      mapOutputStatistics: Array[MapOutputStatistics],
      minNumPostShufflePartitions: Option[Int],
      advisoryTargetPostShuffleInputSize: Long): Option[Array[Int]] = {

    // If minNumPostShufflePartitions is defined, it is possible that we need to use a
    // value less than advisoryTargetPostShuffleInputSize as the target input size of
    // a post shuffle task.
    val targetPostShuffleInputSize = minNumPostShufflePartitions match {
      case Some(numPartitions) =>
        val totalPostShuffleInputSize = mapOutputStatistics.map(_.bytesByPartitionId.sum).sum
        // The max at here is to make sure that when we have an empty table, we
        // only have a single post-shuffle partition.
        // There is no particular reason that we pick 16. We just need a number to
        // prevent maxPostShuffleInputSize from being set to 0.
        val maxPostShuffleInputSize =
          math.max(math.ceil(totalPostShuffleInputSize / numPartitions.toDouble).toLong, 16)
        math.min(maxPostShuffleInputSize, advisoryTargetPostShuffleInputSize)

      case None => advisoryTargetPostShuffleInputSize
    }

    logInfo(s"advisoryTargetPostShuffleInputSize: $advisoryTargetPostShuffleInputSize, " +
      s"targetPostShuffleInputSize $targetPostShuffleInputSize.")

    // Make sure we do get the same number of pre-shuffle partitions for those stages.
    val distinctNumPreShufflePartitions =
      mapOutputStatistics.map(stats => stats.bytesByPartitionId.length).distinct
    // The reason that we are expecting a single value of the number of pre-shuffle partitions
    // is that when we add Exchanges, we set the number of pre-shuffle partitions
    // (i.e. map output partitions) using a static setting, which is the value of
    // spark.sql.shuffle.partitions. Even if two input RDDs are having different
    // number of partitions, they will have the same number of pre-shuffle partitions
    // (i.e. map output partitions).
    assert(
      distinctNumPreShufflePartitions.length == 1,
      "There should be only one distinct value of the number pre-shuffle partitions " +
        "among registered Exchange operator.")
    val numPreShufflePartitions = distinctNumPreShufflePartitions.head

    val partitionStartIndices = ArrayBuffer[Int]()
    // The first element of partitionStartIndices is always 0.
    partitionStartIndices += 0

    var postShuffleInputSize = 0L

    var i = 0
    while (i < numPreShufflePartitions) {
      // We calculate the total size of ith pre-shuffle partitions from all pre-shuffle stages.
      // Then, we add the total size to postShuffleInputSize.
      var j = 0
      while (j < mapOutputStatistics.length) {
        postShuffleInputSize += mapOutputStatistics(j).bytesByPartitionId(i)
        j += 1
      }

      // If the current postShuffleInputSize is equal or greater than the
      // targetPostShuffleInputSize, We need to add a new element in partitionStartIndices.
      if (postShuffleInputSize >= targetPostShuffleInputSize) {
        if (i < numPreShufflePartitions - 1) {
          // Next start index.
          partitionStartIndices += i + 1
        } else {
          // This is the last element. So, we do not need to append the next start index to
          // partitionStartIndices.
        }
        // reset postShuffleInputSize.
        postShuffleInputSize = 0L
      }

      i += 1
    }

    Some(partitionStartIndices.toArray)
  }

  private[sql] def rewriteKeyExpr(keys: Seq[Expression]): Seq[Expression] = {
    var keyExpr: Expression = null
    var width = 0
    keys.foreach { e =>
      e.dataType match {
        case dt: IntegralType if dt.defaultSize <= 8 - width =>
          if (width == 0) {
            if (e.dataType != LongType) {
              keyExpr = Cast(e, LongType)
            } else {
              keyExpr = e
            }
            width = dt.defaultSize
          } else {
            val bits = dt.defaultSize * 8
            // hashCode of Long is (l >> 32) ^ l.toInt, it means the hash code of an long with same
            // value in high 32 bit and low 32 bit will be 0. To avoid the worst case that keys
            // with two same ints have hash code 0, we rotate the bits of second one.
            val rotated = if (e.dataType == IntegerType) {
              // (e >>> 15) | (e << 17)
              BitwiseOr(ShiftRightUnsigned(e, Literal(15)), ShiftLeft(e, Literal(17)))
            } else {
              e
            }
            keyExpr = BitwiseOr(ShiftLeft(keyExpr, Literal(bits)),
              BitwiseAnd(Cast(rotated, LongType), Literal((1L << bits) - 1)))
            width -= bits
          }
        // TODO: support BooleanType, DateType and TimestampType
        case other =>
          return keys
      }
    }
    keyExpr :: Nil
  }
}
