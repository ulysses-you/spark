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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.internal.SQLConf.{WINDOW_GROUP_LIMIT_HASH_BASED_PARTIAL_ENABLED, WINDOW_GROUP_LIMIT_HASH_BASED_PARTIAL_FALLBACK_MEMORY_THRESHOLD, WINDOW_GROUP_LIMIT_THRESHOLD}

/**
 * Benchmark to measure the performance of the partial `WindowGroupLimit` for `ROW_NUMBER`,
 * comparing the sort-based partial against the hash-based (sort-free) partial.
 *
 * The physical plan for a top-k-per-group query is a Partial + Final pair of
 * `WindowGroupLimitExec` straddling the shuffle. The sort-based Partial requires its input to be
 * sorted, so `EnsureRequirements` inserts a full pre-shuffle sort; the hash-based Partial keeps a
 * per-group bounded heap and needs no ordering, avoiding that sort. At runtime the hash-based
 * Partial runs a three-state machine per task, and this benchmark drives scenarios into each of the
 * three states so their relative cost is visible:
 *
 *   - HASH: the limit filters, so the per-group buffers stay bounded and the task consumes all
 *     input in memory without sorting. This is the win -- a cheap hash scan replaces the
 *     pre-shuffle sort. Measured with both few groups and many groups (the buffers still fit) to
 *     show the state
 *     holds as the group count grows.
 *   - PASS_THROUGH: the limit is ineffective (very high cardinality), so the evaluator detects that
 *     the buffers are not filtering and stops, passing rows through. Here the sort-based partial's
 *     pre-shuffle sort is pure loss, while the hash-based partial degrades to a scan.
 *   - SORT_FALLBACK: the limit filters (so the buffers keep growing) but they outgrow the memory
 *     budget, so the evaluator dumps the buffered rows plus the remaining input into a spillable
 *     external sorter. This measures the memory-safe fallback path.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/WindowGroupLimitBenchmark-results.txt".
 * }}}
 */
object WindowGroupLimitBenchmark extends SqlBasedBenchmark {

  private val N = 1024 * 1024 * 20
  private val numPartitions = 11
  private val limit = 100

  /**
   * A scenario is the data shape plus any extra conf that pins the hash-based partial into a given
   * runtime state.
   *
   * @param label       the state this scenario targets.
   * @param bExpr       the expression producing the partition key `b`, which sets the cardinality.
   * @param hashConfs   extra confs applied only to the hash-based case (e.g. a memory cap).
   */
  private case class Scenario(label: String, bExpr: String, hashConfs: Seq[(String, String)] = Nil)

  private val scenarios: Seq[Scenario] = Seq(
    // 1024 contiguous groups of `N / 1024` rows. Contiguous (not `id % 1024`) so that within the
    // pass-through sample each group fills past `limit` and evicts, keeping the survival ratio low;
    // interleaving would leave every group under `limit` in the sample and wrongly trigger
    // PASS_THROUGH. The retained rows fit in memory, so the task stays in HASH and filters.
    Scenario("HASH, few groups (limit filters)", s"id div ${N / 1024}"),
    // Many contiguous groups of 1000 rows (about 20K groups): the limit still filters (survival
    // about 0.1) and the roughly `groups * limit` retained rows fit in memory, so the task stays in
    // HASH without pass-through or fallback. Shows HASH holding as the group count grows 20x.
    Scenario("HASH, many groups (limit filters)", "id div 1000"),
    // Very high cardinality: about 2 rows per group, so nearly every row survives the sample and
    // the evaluator gives up filtering. Goes PASS_THROUGH.
    Scenario("PASS_THROUGH (limit ineffective)", s"id % ${N / 2}"),
    // The same many-group data as the second scenario, but with the buffer memory capped so the
    // retained rows outgrow the budget, forcing the spillable sort. Reusing the data isolates the
    // memory cap as the sole difference between staying in HASH and falling back. Goes
    // SORT_FALLBACK.
    Scenario(
      "SORT_FALLBACK (buffers outgrow memory)",
      "id div 1000",
      Seq(WINDOW_GROUP_LIMIT_HASH_BASED_PARTIAL_FALLBACK_MEMORY_THRESHOLD.key -> "4m")))

  private def benchmarkScenario(scenario: Scenario, path: String): Unit = {
    def f(): Unit = {
      spark.read.parquet(path)
        .selectExpr("ROW_NUMBER() OVER(PARTITION BY b ORDER BY a) AS rn", "a", "b")
        .where(s"rn <= $limit")
        .noop()
    }

    val benchmark = new Benchmark(
      s"ROW_NUMBER top-k, ${scenario.label}", N, minNumIters = 5, output = output)

    benchmark.addCase("WindowGroupLimit off") { _ =>
      withSQLConf(WINDOW_GROUP_LIMIT_THRESHOLD.key -> "-1") {
        f()
      }
    }

    benchmark.addCase("sort-based partial") { _ =>
      withSQLConf(WINDOW_GROUP_LIMIT_HASH_BASED_PARTIAL_ENABLED.key -> "false") {
        f()
      }
    }

    benchmark.addCase("hash-based partial") { _ =>
      // The hash-based partial is enabled at the suite level (see `runBenchmarkSuite`); apply only
      // this scenario's extra confs (e.g. a memory cap that forces SORT_FALLBACK).
      withSQLConf(scenario.hashConfs: _*) {
        f()
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Partial WindowGroupLimit: sort-based vs hash-based (ROW_NUMBER)") {
      // The hash-based partial is off by default, so enable it for the whole suite; the
      // `sort-based partial` case overrides it back to false and the `off` case disables the
      // optimization entirely.
      withSQLConf(WINDOW_GROUP_LIMIT_HASH_BASED_PARTIAL_ENABLED.key -> "true") {
        scenarios.zipWithIndex.foreach { case (scenario, i) =>
          withTempPath { dir =>
            val path = dir.getCanonicalPath + s"/window_group_limit_$i"
            spark.range(0, N, 1, numPartitions)
              .selectExpr("id as a", s"${scenario.bExpr} as b")
              .write.mode("overwrite")
              .parquet(path)
            benchmarkScenario(scenario, path)
          }
        }
      }
    }
  }
}
