package org.apache.spark.sql.execution.adaptive

import java.util.{HashMap => JHashMap, Map => JMap}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{MapOutputStatistics, SimpleFutureAction, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{Sort, SparkPlan}
import org.apache.spark.sql.execution.aggregate.TungstenAggregate
import org.apache.spark.sql.execution.exchange.{BroadcastExchange, ShuffleExchange}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.types.LongType

/**
 * A physical plan tree is divided into a DAG tree of QueryFragment.
 * An QueryFragment is a basic execution unit that could be optimized
 * According to statistics of children fragments.
 */
case class QueryFragment (children: Seq[QueryFragment], isRoot: Boolean = false) extends SparkPlan {

  private[this] var exchange: ShuffleExchange = null

  private[this] var rootPlan: SparkPlan = null

  private[this] var fragmentInput: FragmentInput = null

  private[this] val fragmentsIndex: JMap[QueryFragment, Integer] =
    new JHashMap[QueryFragment, Integer](children.size)

  private[this] val shuffleDependencies =
    new Array[ShuffleDependency[Int, InternalRow, InternalRow]](children.size)

  private[this] val submittedStageFutures =
    new Array[SimpleFutureAction[MapOutputStatistics]](children.size)

  private[this] val mapOutputStatistics = new Array[MapOutputStatistics](children.size)

  private[this] val advisoryTargetPostShuffleInputSize: Long =
    sqlContext.conf.targetPostShuffleInputSize

  override def output: Seq[Attribute] = executedPlan.output

  private[this] def executedPlan: SparkPlan = if (isRoot) {
    rootPlan
  } else {
    exchange
  }

  private[sql] def setFragmentInput(fragmentInput: FragmentInput) = {
    this.fragmentInput = fragmentInput
  }

  private[sql] def getFragmentInput() = fragmentInput

  private[sql] def setExchange(exchange: ShuffleExchange) = {
    this.exchange = exchange
  }

  private[sql] def getExchange(): ShuffleExchange = exchange

  private[sql] def setRootPlan(plan: SparkPlan) = {
    this.rootPlan = plan
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (!children.isEmpty) {
      submitChildrenFragments()
      val executedPlan = if (isRoot) {
        rootPlan
      } else {
        exchange
      }
      // Optimize plan
      val optimizedPlan = executedPlan.transformDown {
        case operator @ SortMergeJoin(leftKeys, rightKeys, condition,
        left@Sort(_, _, _, _), right@Sort(_, _, _, _)) => {
          logInfo("Begin optimize join, operator =\n" + operator.toString)
          val newOperator = optimizeJoin(operator, left, right)
          logInfo("After optimize join, operator =\n" + newOperator.toString)
          newOperator
        }

        case agg @ TungstenAggregate(_, _, _, _, _, _, input @ FragmentInput(_))
          if (!input.isOptimized())=> {
          optimizeAggregate(agg, input)
        }

        case operator: SparkPlan => operator
      }

      if (isRoot) {
        rootPlan = optimizedPlan
      } else {
        exchange = optimizedPlan.asInstanceOf[ShuffleExchange]
      }
    }
    if (isRoot) {
      rootPlan.execute()
    } else {
      null
    }
  }

  private[this] def minNumPostShufflePartitions: Option[Int] = {
    val minNumPostShufflePartitions = sqlContext.conf.minNumPostShufflePartitions
    if (minNumPostShufflePartitions > 0) Some(minNumPostShufflePartitions) else None
  }

  private[this] def submitChildrenFragments() = {
    // Submit all map stages
    val numExchanges = children.size
    var i = 0
    while (i < numExchanges) {
      children(i).execute()
      fragmentsIndex.put(children(i), i)
      val exchange = sqlContext.codegenForExecution.execute(children(i).getExchange())
        .asInstanceOf[ShuffleExchange]
      logInfo("== submit plan ==\n" + exchange.toString)
      val shuffleDependency = exchange.prepareShuffleDependency()
      shuffleDependencies(i) = shuffleDependency
      if (shuffleDependency.rdd.partitions.length != 0) {
        // submitMapStage does not accept RDD with 0 partition.
        // So, we will not submit this dependency.
        submittedStageFutures(i) = sqlContext.sparkContext.submitMapStage(shuffleDependency)
      } else {
        submittedStageFutures(i) = null
      }

      i += 1
    }

    // Wait for the finishes of those submitted map stages.
    var j = 0
    while (j < submittedStageFutures.length) {
      // This call is a blocking call. If the stage has not finished, we will wait at here.
      if (submittedStageFutures(j) != null) {
        mapOutputStatistics(j) = submittedStageFutures(j).get()
      } else {
        mapOutputStatistics(j) = null
      }

      j += 1
    }
  }

  private[this] def optimizeAggregate(agg: SparkPlan, input: FragmentInput): SparkPlan = {
    val childFragments = Seq(input.childFragment)
    val aggStatistics = new ArrayBuffer[MapOutputStatistics]()
    var i = 0
    while(i < childFragments.length) {
      val statistics = mapOutputStatistics(fragmentsIndex.get(childFragments(i)))
      if (statistics != null) {
        aggStatistics += statistics
      }
      i += 1
    }
    val partitionStartIndices =
      if (aggStatistics.length == 0) {
        None
      } else {
        Utils.estimatePartitionStartIndices(aggStatistics.toArray, minNumPostShufflePartitions,
          advisoryTargetPostShuffleInputSize)
      }
    val shuffledRowRdd= childFragments(0).getExchange().preparePostShuffleRDD(
      shuffleDependencies(fragmentsIndex.get(childFragments(0))), partitionStartIndices)
    childFragments(0).getFragmentInput().setShuffleRdd(shuffledRowRdd)
    childFragments(0).getFragmentInput().setOptimized()
    agg
  }

  private[this] def optimizeJoin(joinPlan: SortMergeJoin, left: Sort, right: Sort): SparkPlan = {
    // TODO Optimize skew join
    val childFragments = Utils.findChildFragment(joinPlan)
    assert(childFragments.length == 2)
    val joinStatistics = new ArrayBuffer[MapOutputStatistics]()
    val childSizeInBytes = new Array[Long](childFragments.length)
    var i = 0
    while(i < childFragments.length) {
      val statistics = mapOutputStatistics(fragmentsIndex.get(childFragments(i)))
      if (statistics != null) {
        joinStatistics += statistics
        childSizeInBytes(i) = statistics.bytesByPartitionId.sum
      } else {
        childSizeInBytes(i) = 0
      }
      i += 1
    }
    val partitionStartIndices =
      if (joinStatistics.length == 0) {
        None
      } else {
        Utils.estimatePartitionStartIndices(joinStatistics.toArray, minNumPostShufflePartitions,
          advisoryTargetPostShuffleInputSize)
      }

    val leftFragment = childFragments(0)
    val rightFragment = childFragments(1)
    val leftShuffledRowRdd= leftFragment.getExchange().preparePostShuffleRDD(
      shuffleDependencies(fragmentsIndex.get(leftFragment)), partitionStartIndices)
    val rightShuffledRowRdd = rightFragment.getExchange().preparePostShuffleRDD(
      shuffleDependencies(fragmentsIndex.get(rightFragment)), partitionStartIndices)

    leftFragment.getFragmentInput().setShuffleRdd(leftShuffledRowRdd)
    leftFragment.getFragmentInput().setOptimized()
    rightFragment.getFragmentInput().setShuffleRdd(rightShuffledRowRdd)
    rightFragment.getFragmentInput().setOptimized()

    var newOperator: SparkPlan = joinPlan

    if (sqlContext.conf.autoBroadcastJoinThreshold > 0) {
      val leftSizeInBytes = childSizeInBytes(0)
      val rightSizeInBytes = childSizeInBytes(1)
      if (leftSizeInBytes <= sqlContext.conf.autoBroadcastJoinThreshold) {
        val keys = Utils.rewriteKeyExpr(joinPlan.leftKeys)
        val sameTypes = joinPlan.leftKeys.map(_.dataType) == joinPlan.rightKeys.map(_.dataType)
        val canJoinKeyFitWithinLong: Boolean = 
          sameTypes && keys.length == 1 && keys.head.dataType.isInstanceOf[LongType]
        
        newOperator = BroadcastHashJoin(
          joinPlan.leftKeys, joinPlan.rightKeys, Inner, BuildLeft, joinPlan.condition,
          BroadcastExchange(HashedRelationBroadcastMode(
            canJoinKeyFitWithinLong,
            keys,
            left.child.output), 
            left.child),
          right.child)
      } else if (rightSizeInBytes <= sqlContext.conf.autoBroadcastJoinThreshold) {
        val keys = Utils.rewriteKeyExpr(joinPlan.rightKeys)
        val sameTypes = joinPlan.leftKeys.map(_.dataType) == joinPlan.rightKeys.map(_.dataType)
        val canJoinKeyFitWithinLong: Boolean =
          sameTypes && keys.length == 1 && keys.head.dataType.isInstanceOf[LongType]
        newOperator = BroadcastHashJoin(
          joinPlan.leftKeys, joinPlan.rightKeys, Inner, BuildRight, joinPlan.condition,
          left.child,
          BroadcastExchange(HashedRelationBroadcastMode(
            canJoinKeyFitWithinLong,
            keys,
            right.child.output), 
            right.child))
      } 
    }
    newOperator
  }

  /** Returns a string representation of the nodes in this tree */
  override def treeString: String = 
    executedPlan.generateTreeString(0, Nil, new StringBuilder).toString

  override def simpleString: String = "QueryFragment"
}
