package org.apache.spark.sql.execution.adaptive

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingDeque, BlockingQueue}
import java.util.{HashMap => JHashMap, Map => JMap}

import scala.concurrent.ExecutionContext.Implicits.global
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
trait QueryFragment extends SparkPlan {

  def children: Seq[QueryFragment]
  def isRoot: Boolean
  def id: Long

  private[this] var exchange: ShuffleExchange = null

  protected[this] var rootPlan: SparkPlan = null

  private[this] var fragmentInput: FragmentInput = null

  private[this] var parentFragment: QueryFragment = null

  var nextChildIndex: Int = 0

  private[this] val fragmentsIndex: JMap[QueryFragment, Integer] =
    new JHashMap[QueryFragment, Integer](children.size)

  private[this] val shuffleDependencies =
    new Array[ShuffleDependency[Int, InternalRow, InternalRow]](children.size)

  private[this] val mapOutputStatistics = new Array[MapOutputStatistics](children.size)

  private[this] val advisoryTargetPostShuffleInputSize: Long =
    sqlContext.conf.targetPostShuffleInputSize

  override def output: Seq[Attribute] = executedPlan.output

  private[this] def executedPlan: SparkPlan = if (isRoot) {
    rootPlan
  } else {
    exchange
  }

  private[sql] def setParentFragment(fragment: QueryFragment) = {
    this.parentFragment = fragment
  }

  private[sql] def getParentFragment() = parentFragment

  private[sql] def setFragmentInput(fragmentInput: FragmentInput) = {
    this.fragmentInput = fragmentInput
  }

  private[sql] def getFragmentInput() = fragmentInput

  private[sql] def setExchange(exchange: ShuffleExchange) = {
    this.exchange = exchange
  }

  private[sql] def getExchange(): ShuffleExchange = exchange

  private[sql] def setRootPlan(root: SparkPlan) = {
    this.rootPlan = root
  }

  protected[sql] def isAvailable: Boolean = nextChildIndex >= children.size


  protected def doExecute(): RDD[InternalRow] = null

  protected[sql] def adaptiveExecute(): (ShuffleDependency[Int, InternalRow, InternalRow],
    SimpleFutureAction[MapOutputStatistics]) = synchronized {
    val executedPlan = sqlContext.codegenForExecution.execute(exchange)
      .asInstanceOf[ShuffleExchange]
    logInfo(s"== Submit Query Fragment ${id} Physical plan ==")
    logInfo(stringOrError(executedPlan.toString))
    val shuffleDependency = executedPlan.prepareShuffleDependency()
    if (shuffleDependency.rdd.partitions.length != 0) {
      val futureAction: SimpleFutureAction[MapOutputStatistics] =
        sqlContext.sparkContext.submitMapStage[Int, InternalRow, InternalRow](shuffleDependency)
      (shuffleDependency, futureAction)
//      futureAction.onComplete {
//        case scala.util.Success(statistics) =>
//          logInfo(s"Query Fragment ${id} finished")
//          this.getParentFragment().setChildCompleted(this, shuffleDependency, statistics)
//        case scala.util.Failure(exception) =>
//          logInfo(s"Query Fragment ${id} failed")
//          this.getParentFragment().stageFailed(exception)
//          throw exception
//      }
    } else {
      (shuffleDependency, null)
      //this.getParentFragment().setChildCompleted(this, shuffleDependency, null)
    }
  }

  protected[sql] def stageFailed(exception: Throwable): Unit = synchronized {
    this.parentFragment.stageFailed(exception)
  }

  protected def stringOrError[A](f: => A): String =
    try f.toString catch { case e: Throwable => e.toString }

  protected[sql] def setChildCompleted(
      child: QueryFragment,
      shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow],
      statistics: MapOutputStatistics): Unit = synchronized {
    fragmentsIndex.put(child, this.nextChildIndex)
    shuffleDependencies(this.nextChildIndex) = shuffleDependency
    mapOutputStatistics(this.nextChildIndex) = statistics
    this.nextChildIndex += 1
  }

  protected[sql] def optimizeOperator(): Unit = synchronized {
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

  private[this] def minNumPostShufflePartitions: Option[Int] = {
    val minNumPostShufflePartitions = sqlContext.conf.minNumPostShufflePartitions
    if (minNumPostShufflePartitions > 0) Some(minNumPostShufflePartitions) else None
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

case class RootQueryFragment (
    children: Seq[QueryFragment],
    id: Long,
    isRoot: Boolean = false) extends QueryFragment {

  private[this] var isThrowException = false

  private[this] var exception: Throwable = null

  private[this] val stopped = new AtomicBoolean(false)

  protected[sql] override def stageFailed(exception: Throwable): Unit = {
    isThrowException = true
    this.exception = exception
    stopped.set(true)
  }

  private val eventQueue: BlockingQueue[QueryFragment] = new LinkedBlockingDeque[QueryFragment]()

  protected def executeFragment(child: QueryFragment) = {
    val (shuffleDependency, futureAction) = child.adaptiveExecute()
    val parent = child.getParentFragment()
    if (futureAction != null) {
      futureAction.onComplete {
        case scala.util.Success(statistics) =>
          logInfo(s"Query Fragment ${id} finished")
          parent.setChildCompleted(child, shuffleDependency, statistics)
          if (parent.isAvailable) {
            eventQueue.add(parent)
          }
        case scala.util.Failure(exception) =>
          logInfo(s"Query Fragment ${id} failed, exception is ${exception}")
          parent.stageFailed(exception)
      }
    } else {
      parent.setChildCompleted(child, shuffleDependency, null)
      if (parent.isAvailable) {
        eventQueue.add(parent)
      }
    }
  }


  protected override def doExecute(): RDD[InternalRow] = {
    assert(isRoot == true)
    isThrowException = false
    stopped.set(false)
    val children = Utils.findLeafFragment(this)
    if (!children.isEmpty) {
      children.foreach { child => executeFragment(child) }
    } else {
      stopped.set(true)
    }

    val executeThread = new Thread("Fragment execute") {
      setDaemon(true)

      override def run(): Unit = {
        while (!stopped.get) {
          val fragment = eventQueue.take()
          fragment.optimizeOperator()
          if (fragment.isInstanceOf[RootQueryFragment]) {
            stopped.set(true)
          } else {
            executeFragment(fragment)
          }

        }
      }
    }
    executeThread.start()
    executeThread.join()
    if (isThrowException) {
      assert(this.exception != null)
      throw exception
    } else {
      rootPlan.execute()
    }
  }

  /** Returns a string representation of the nodes in this tree */
  override def treeString: String =
    rootPlan.generateTreeString(0, Nil, new StringBuilder).toString

  override def simpleString: String = "QueryFragment"

}

case class UnaryQueryFragment (
    children: Seq[QueryFragment],
    id: Long,
    isRoot: Boolean = false) extends QueryFragment {}
