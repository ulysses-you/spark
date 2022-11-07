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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.{AQEPropagateEmptyRelation, LogicalQueryStage}

/**
 * Convert [[NonEmpty]] to [[LocalRelation]] if all children do not reduce the output row count.
 */
object OptimizeNonEmpty extends Rule[LogicalPlan] {
  def nonEmpty(plan: LogicalPlan): Boolean = plan match {
    case l: LogicalQueryStage => AQEPropagateEmptyRelation.nonEmpty(l)
    case _: Union => plan.children.exists(nonEmpty)
    case Join(left, right, joinType, condition, _) =>
      joinType match {
        case LeftOuter => nonEmpty(left)
        case RightOuter => nonEmpty(right)
        case FullOuter => nonEmpty(left) || nonEmpty(right)
        case Inner | Cross if condition.isEmpty => nonEmpty(left) && nonEmpty(right)
        case _ => false
      }
    case p: Project => nonEmpty(p.child)
    case agg: Aggregate if agg.groupingExpressions.isEmpty => true
    case agg: Aggregate => nonEmpty(agg.child)
    case s: Sort => nonEmpty(s.child)
    case w: Window => nonEmpty(w.child)
    case LocalLimit(IntegerLiteral(0), _) => false
    case GlobalLimit(IntegerLiteral(0), _) => false
    case l: LocalLimit => nonEmpty(l.child)
    case g: GlobalLimit => nonEmpty(g.child)
    case g: Generate if g.outer => nonEmpty(g.child)
    case e: Expand => nonEmpty(e.child)
    case r: RepartitionOperation => nonEmpty(r.child)
    case r: RebalancePartitions => nonEmpty(r.child)
    case _ => false
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case n: NonEmpty if nonEmpty(n.child) =>
      LocalRelation.apply(n.output, InternalRow.fromSeq(Seq(true)) :: Nil)
    case _ => plan
  }
}
