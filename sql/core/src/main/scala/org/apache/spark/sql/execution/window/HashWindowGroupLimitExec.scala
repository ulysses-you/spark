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

package org.apache.spark.sql.execution.window

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * A pre-shuffle partial counterpart of the [[Partial]] [[WindowGroupLimitExec]] that does not
 * require its input to be sorted, and therefore avoids the pre-shuffle sort that
 * `EnsureRequirements` would otherwise insert for the sort-based partial limit. It only filters
 * out rows that cannot enter the global top-k, leaving the exact rank computation to the final
 * [[WindowGroupLimitExec]] after the shuffle.
 *
 * It keeps, per group, the `limit` rows smallest by `orderSpec` in an in-memory bounded queue.
 * This is only valid for `RowNumber`: a row survives only if its position within the group is
 * `<= limit`, so retaining the `limit` smallest rows per group never drops a row that could have a
 * final row number `<= limit`. `RANK`/`DENSE_RANK` cannot use this bound and keep the sort-based
 * partial.
 *
 * At runtime it adaptively degrades (see [[HashWindowGroupLimitEvaluatorFactory]]):
 *   - PASS_THROUGH: if the limit filters too little, stop filtering and pass rows through.
 *   - SORT_FALLBACK: if the buffers hold too many rows, fall back to a spillable sort-based limit.
 *
 * @param partitionSpec Should be the same as [[WindowExec#partitionSpec]].
 * @param orderSpec Should be the same as [[WindowExec#orderSpec]].
 * @param rankLikeFunction The rank-like function, must be `RowNumber`.
 * @param limit The limit for rank value.
 * @param child The child spark plan.
 */
case class HashWindowGroupLimitExec(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    limit: Int,
    child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numFallbackTasks" ->
      SQLMetrics.createMetric(sparkContext, "number of sort fallback tasks"),
    "numPassThroughTasks" ->
      SQLMetrics.createMetric(sparkContext, "number of pass-through tasks"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  protected override def doExecute(): RDD[InternalRow] = {
    val evaluatorFactory =
      new HashWindowGroupLimitEvaluatorFactory(
        partitionSpec,
        orderSpec,
        limit,
        child.output,
        conf.windowGroupLimitHashBasedPartialPassThroughRatio,
        conf.windowGroupLimitHashBasedPartialPassThroughSampleRows,
        conf.windowGroupLimitHashBasedPartialFallbackMemoryThreshold,
        conf.enableRadixSort,
        longMetric("numOutputRows"),
        longMetric("numFallbackTasks"),
        longMetric("numPassThroughTasks"),
        longMetric("spillSize"))

    if (conf.usePartitionEvaluator) {
      child.execute().mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      child.execute().mapPartitionsWithIndexInternal { (index, rowIterator) =>
        val evaluator = evaluatorFactory.createEvaluator()
        evaluator.eval(index, rowIterator)
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): HashWindowGroupLimitExec =
    copy(child = newChild)
}
