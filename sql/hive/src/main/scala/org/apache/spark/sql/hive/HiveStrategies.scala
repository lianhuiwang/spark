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

package org.apache.spark.sql.hive

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive.execution._

private[hive] trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkPlanner =>

  val sparkSession: SparkSession

  object Scripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ScriptTransformation(input, script, output, child, ioschema) =>
        val hiveIoSchema = HiveScriptIOSchema(ioschema)
        ScriptTransformation(input, script, output, planLater(child), hiveIoSchema) :: Nil
      case _ => Nil
    }
  }

  object DataSinks extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.InsertIntoTable(
          table: MetastoreRelation, partition, child, overwrite, ifNotExists) =>
        execution.InsertIntoHiveTable(
          table, partition, planLater(child), overwrite, ifNotExists) :: Nil
      case hive.InsertIntoHiveTable(
          table: MetastoreRelation, partition, child, overwrite, ifNotExists) =>
        execution.InsertIntoHiveTable(
          table, partition, planLater(child), overwrite, ifNotExists) :: Nil
      case _ => Nil
    }
  }

  /**
   * Retrieves data using a HiveTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HiveTableScans extends Strategy with Logging {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: MetastoreRelation) =>
        // Filter out all predicates that only deal with partition keys, these are given to the
        // hive table scan operator to be used for partition pruning.
        val partitionKeyIds = AttributeSet(relation.partitionKeys)
        val otherPredicates = if (partitionKeyIds.isEmpty) {
          predicates
        } else {
          predicates.filter { predicate => predicate.references.isEmpty ||
            !predicate.references.subsetOf(partitionKeyIds) }
        }
        val pruningPredicates = if (partitionKeyIds.isEmpty) {
          Seq.empty[Expression]
        } else {
          predicates.map(pruneNoPartitionKeys(_, partitionKeyIds)).filterNot(TrueLiteral.equals(_))
        }

        logDebug("for HiveTableScans partitionKeyIds=" + partitionKeyIds + ", pruningPredicates="
          + pruningPredicates.mkString("#") + ", otherPredicates=" + otherPredicates.mkString("#"))

        pruneFilterProject(
          projectList,
          otherPredicates,
          identity[Seq[Expression]],
          HiveTableScanExec(_, relation, pruningPredicates)(sparkSession)) :: Nil
      case _ =>
        Nil
    }

    def pruneNoPartitionKeys(
        predicate: Expression,
        partitionKeyIds: AttributeSet): Expression = {
      predicate match {
        case And(left, right) =>
          val newLeft = pruneNoPartitionKeys(left, partitionKeyIds)
          val newRight = pruneNoPartitionKeys(right, partitionKeyIds)
          if (TrueLiteral.equals(newLeft) && TrueLiteral.equals(newRight)) {
            TrueLiteral
          } else if (TrueLiteral.equals(newLeft)) {
            newRight
          } else if (TrueLiteral.equals(newRight)) {
            newLeft
          } else {
            And(newLeft, newRight)
          }

        case Or(left, right) =>
          val newLeft = pruneNoPartitionKeys(left, partitionKeyIds)
          val newRight = pruneNoPartitionKeys(right, partitionKeyIds)
          if (TrueLiteral.equals(newLeft) || TrueLiteral.equals(newRight)) {
            TrueLiteral
          } else {
            Or(newLeft, newRight)
          }

        case Not(child) =>
          val newChild = pruneNoPartitionKeys(child, partitionKeyIds)
          if (newChild.equals(TrueLiteral)) {
            TrueLiteral
          } else {
            Not(newChild)
          }

        case other =>
         if (other.references.nonEmpty && other.references.subsetOf(partitionKeyIds)) {
           other
         } else {
           TrueLiteral
         }
      }
    }
  }

}
