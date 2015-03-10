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

package org.apache.spark.rdd

import java.io.{ObjectOutputStream, IOException}

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleHandle

private[spark] object JoinType extends Enumeration {

  type JoinType = Value

  val INNER, LEFTOUTER, RIGHTOUTER = Value
}

private[spark] case class ShuffleJoinSplitDep(handle: ShuffleHandle) extends Serializable

private[spark] class JoinPartition(idx: Int, val left: ShuffleJoinSplitDep, val right: ShuffleJoinSplitDep)
  extends Partition with Serializable {
  override val index: Int = idx

  override def hashCode(): Int = idx
}

private[spark] class SortMergeJoin[K, L, R, PAIR <: Product2[_, _]](
    left: RDD[(K, L)], right: RDD[(K, R)], part: Partitioner, joinType: JoinType.Value)
    (implicit kt: ClassTag[K], lt: ClassTag[L], rt: ClassTag[R], keyOrdering: Ordering[K])
  extends RDD[(K, PAIR)](left.context, Nil) with Logging {

  // Ordering is necessary. SortMergeJoin needs to Sort by Key
  require(keyOrdering != null, "No implicit Ordering defined for " + kt.runtimeClass)

  private var serializer: Option[Serializer] = None

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): SortMergeJoin[K, L, R, PAIR] = {
    this.serializer = Option(serializer)
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(left, right).map { rdd: RDD[_ <: Product2[K, _]] =>
      logDebug("Adding shuffle dependency with " + rdd)
      new ShuffleDependency[K, Any, Any](rdd, part, serializer, Some(keyOrdering))
    }
  }

  private def getJoinSplitDep(rdd: RDD[_], index: Int, dep: Dependency[_]): ShuffleJoinSplitDep =
    dep match {
      case s: ShuffleDependency[_, _, _] =>
        new ShuffleJoinSplitDep(s.shuffleHandle)
    }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- 0 until array.size) {
      array(i) = new JoinPartition(i,
        getJoinSplitDep(left, i, dependencies(0)),
        getJoinSplitDep(right, i, dependencies(1)))
    }
    array
  }

  override val partitioner: Some[Partitioner] = Some(part)

  private def mergeValues[K,V](sorted: Iterator[Product2[K, V]]): Iterator[(K, Iterable[V])]={
    new Iterator[(K, Iterable[V])] {

      var cur:Product2[K, V] = null
      var _hasNext: Boolean = false

      def hasNext: Boolean = {
        if (_hasNext) {
          true
        } else if (sorted.hasNext) {
          cur = sorted.next()
          _hasNext = true
          true
        } else {
          false
        }
      }

      def next(): (K, Iterable[V]) = {
        val itr = new Iterable[V] {
          override def iterator = new Iterator[V] {

            def hasNext: Boolean = {
              if (_hasNext) {
                true
              } else if (sorted.hasNext) {
                _hasNext = true
                val elem = sorted.next()
                var keyIsSame:Boolean = false
                if (keyOrdering.compare(elem._1, cur._1) == 0) {
                  keyIsSame = true
                }
                cur = elem
                keyIsSame
              } else {
                false
              }
            }

            def next(): V = {
              _hasNext = false
              cur._2
            }
          }
        }
        (cur._1, itr)
      }
    }
  }

  private def internalCompute(leftIter: Iterator[Product2[K, L]], rightIter: Iterator[Product2[K, R]]):
  Iterator[(K, (Iterator[L], Iterator[R]))] = {
    new Iterator[(K, (Iterator[L], Iterator[R]))] {

      var leftNext:Product2[K, L] = null
      var rightNext:Product2[K, R] = null
      var currentKey:K = null

      def hasNext: Boolean = {
        if (leftNext != null) {
          true
        } else if (leftIter.hasNext){
          leftNext = leftIter.next()
          true
        } else {
          false
        }
      }

      def next(): (K, (Iterator[L], Iterator[R])) = {
        currentKey = leftNext._1
        var keyIsSame:Boolean = true
        val leftIter1 = new Iterator[L] {
          def hasNext: Boolean = {
            if (leftNext != null) {
              true
            } else if (leftIter.hasNext){
              leftNext = leftIter.next()
              if (keyOrdering.compare(currentKey, leftNext._1) == 0) {
                true
              } else {
                false
              }
            } else {
              false
            }
          }

          def next():L = {
            val leftVal = leftNext._2
            leftNext = null
            leftVal
          }
        }

        val rightIter1 = new Iterator[R] {
          def hasNext: Boolean = {
            if (rightNext == null) {
              if (rightIter.hasNext){
                rightNext = rightIter.next()
              } else {
                false
              }
            }

            var comp = keyOrdering.compare(currentKey, rightNext._1)
            while (comp > 0) {
              if (rightIter.hasNext){
                rightNext = rightIter.next()
              } else {
                false
              }
            }
            if (comp == 0) {
              true
            } else {
              false
            }
          }

          def next():R = {
            val rightVal = rightNext._2
            rightNext = null
            rightVal
          }
        }
        (currentKey,(leftIter1, rightIter1))
      }
    }
  }
  override def compute(s: Partition, context: TaskContext): Iterator[(K, (Iterator[L], Iterator[R]))] = {
    val sparkConf = SparkEnv.get.conf
    val split = s.asInstanceOf[JoinPartition]
    val leftIter = SparkEnv.get.shuffleManager
      .getReader(split.left.handle,  split.index, split.index + 1, context)
      .read().asInstanceOf[Iterator[Product2[K, L]]]
    val rightIter = SparkEnv.get.shuffleManager
      .getReader(split.right.handle,  split.index, split.index + 1, context)
      .read().asInstanceOf[Iterator[Product2[K, R]]]
    internalCompute(leftIter,  rightIter)
  }
}
