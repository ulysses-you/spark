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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.window.{HashWindowGroupLimitExec, Partial, WindowGroupLimitExec}
import org.apache.spark.sql.functions.{lit, row_number}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class RemoveRedundantWindowGroupLimitsSuiteBase
    extends SharedSparkSession
    with AdaptiveSparkPlanHelper {

  private def checkNumWindowGroupLimits(df: DataFrame, count: Int): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(collectWithSubqueries(plan) { case exec: WindowGroupLimitExec => exec }.length == count)
  }

  private def checkWindowGroupLimits(query: String, count: Int): Unit = {
    val df = sql(query)
    checkNumWindowGroupLimits(df, count)
    val result = df.collect()
    checkAnswer(df, result)
  }

  test("remove redundant WindowGroupLimits") {
    withTempView("t") {
      spark.range(0, 100).withColumn("value", lit(1)).createOrReplaceTempView("t")
      val query1 =
        """
          |SELECT *
          |FROM (
          |    SELECT id, rank() OVER w AS rn
          |    FROM t
          |    GROUP BY id
          |    WINDOW w AS (PARTITION BY id ORDER BY max(value))
          |)
          |WHERE rn < 3
          |""".stripMargin
      checkWindowGroupLimits(query1, 1)

      val query2 =
        """
          |SELECT *
          |FROM (
          |    SELECT id, rank() OVER w AS rn
          |    FROM t
          |    GROUP BY id
          |    WINDOW w AS (ORDER BY max(value))
          |)
          |WHERE rn < 3
          |""".stripMargin
      checkWindowGroupLimits(query2, 2)
    }
  }

  test("SPARK-58324: hash-based partial has no pre-shuffle sort") {
    import testImplicits._
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_THRESHOLD.key -> "1000") {
      val df = spark.range(0, 100)
        .selectExpr("id", "id % 7 AS key", "id AS order")
        .toDF()
      val window = org.apache.spark.sql.expressions.Window
        .partitionBy($"key").orderBy($"order")

      def partialAndSortBelow(hashBased: Boolean): (SparkPlan, Boolean) = {
        withSQLConf(
          SQLConf.WINDOW_GROUP_LIMIT_HASH_BASED_PARTIAL_ENABLED.key -> hashBased.toString) {
          val query = df.withColumn("rn", row_number().over(window)).where($"rn" <= 2)
          val plan = query.queryExecution.executedPlan
          val partials = collectWithSubqueries(plan) {
            case e: WindowGroupLimitExec if e.mode == Partial => e
            case e: HashWindowGroupLimitExec => e
          }
          // Exactly one partial node below the final node, straddling the shuffle.
          assert(partials.length == 1)
          val partial = partials.head
          val hasSort = find(partial)(_.isInstanceOf[SortExec]).isDefined
          (partial, hasSort)
        }
      }

      // The sort-based partial forces a pre-shuffle sort; the hash-based partial does not.
      val (sortedPartial, sortedHasSort) = partialAndSortBelow(hashBased = false)
      assert(sortedPartial.isInstanceOf[WindowGroupLimitExec])
      assert(sortedHasSort, "sort-based partial should have a pre-shuffle SortExec below it")

      val (hashPartial, hashHasSort) = partialAndSortBelow(hashBased = true)
      assert(hashPartial.isInstanceOf[HashWindowGroupLimitExec])
      assert(!hashHasSort, "hash-based partial should not have a pre-shuffle SortExec below it")
    }
  }

  test("SPARK-58324: hash-based partial converts to sort-based when child is already ordered") {
    import testImplicits._
    withSQLConf(
      SQLConf.WINDOW_GROUP_LIMIT_THRESHOLD.key -> "1000",
      SQLConf.WINDOW_GROUP_LIMIT_HASH_BASED_PARTIAL_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // Sort the input the way the partial needs, so the child ordering already satisfies
      // (partitionSpec, orderSpec) and the physical rule should convert the hash-based partial
      // back to a sort-based one.
      val df = spark.range(0, 100)
        .selectExpr("id", "id % 7 AS key", "id AS order")
        .toDF()
        .sortWithinPartitions($"key", $"order")
      val window = org.apache.spark.sql.expressions.Window
        .partitionBy($"key").orderBy($"order")
      val query = df.withColumn("rn", row_number().over(window)).where($"rn" <= 2)
      val plan = query.queryExecution.executedPlan
      assert(collectWithSubqueries(plan) {
        case e: WindowGroupLimitExec if e.mode == Partial => e
      }.nonEmpty, "partial should be converted to the sort-based node when the child is ordered")
      assert(collectWithSubqueries(plan) { case e: HashWindowGroupLimitExec => e }.isEmpty)
      checkAnswer(query, query.collect())
    }
  }
}

class RemoveRedundantWindowGroupLimitsSuite extends RemoveRedundantWindowGroupLimitsSuiteBase
  with DisableAdaptiveExecutionSuite

class RemoveRedundantWindowGroupLimitsSuiteAE extends RemoveRedundantWindowGroupLimitsSuiteBase
  with EnableAdaptiveExecutionSuite
