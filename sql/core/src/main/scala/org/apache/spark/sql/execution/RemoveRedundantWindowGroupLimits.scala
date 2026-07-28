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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{Ascending, Expression, SortOrder}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.window.{Final, HashWindowGroupLimitExec, Partial, WindowGroupLimitExec}

/**
 * Cleans up partial window group limit nodes after `EnsureRequirements`:
 *
 *   - Removes a partial node (sort-based [[WindowGroupLimitExec]] or hash-based
 *     [[HashWindowGroupLimitExec]]) that is redundant because its child already satisfies the
 *     final node's required child distribution (so no shuffle sits between them).
 *   - Converts a hash-based partial node into a sort-based one when the child is already sorted the
 *     way the partial needs. The hash-based node exists to avoid an otherwise-required pre-shuffle
 *     sort; if that sort is already present for free, the sort-based node is preferable because it
 *     can stop scanning a group early once the limit is reached.
 */
object RemoveRedundantWindowGroupLimits extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = plan transform {
    case outer @ WindowGroupLimitExec(
    _, _, _, _, Final, WindowGroupLimitExec(_, _, _, _, Partial, child))
      if child.outputPartitioning.satisfies(outer.requiredChildDistribution.head) =>
      outer.withNewChildren(Seq(child))

    case outer @ WindowGroupLimitExec(
    _, _, _, _, Final, HashWindowGroupLimitExec(_, _, _, _, child))
      if child.outputPartitioning.satisfies(outer.requiredChildDistribution.head) =>
      outer.withNewChildren(Seq(child))

    // A hash-based partial exists only to avoid a pre-shuffle sort. If its child is already sorted
    // the way it needs (e.g. an explicit sort, or a sort required by another operator), convert it
    // to the sort-based partial, which can stop scanning a group early once the limit is reached.
    // This matches the node directly rather than through its `Final` parent because, after
    // `EnsureRequirements`, a shuffle (and possibly a sort) sits between the two nodes.
    case HashWindowGroupLimitExec(partitionSpec, orderSpec, rankLikeFunction, limit, child)
      if isChildAlreadyOrdered(partitionSpec, orderSpec, child) =>
      WindowGroupLimitExec(partitionSpec, orderSpec, rankLikeFunction, limit, Partial, child)
  }

  private def isChildAlreadyOrdered(
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      child: SparkPlan): Boolean = {
    val requiredOrdering = partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec
    SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)
  }

}
