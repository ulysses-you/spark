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

import java.util.Properties

import org.apache.spark.{SparkConf, TaskContext, TaskContextImpl}
import org.apache.spark.internal.config.BUFFER_PAGESIZE
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryConsumer, TestMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

/**
 * Evaluator-level tests for [[HashWindowGroupLimitEvaluatorFactory]] that exercise interactions
 * with the [[TaskMemoryManager]] which cannot be triggered deterministically through the DataFrame
 * API -- most importantly, the manager asking the row-buffer consumer to spill.
 */
class HashWindowGroupLimitEvaluatorFactorySuite extends SharedSparkSession {

  // A single non-null `Int` column named `a`, used as the order key with an empty partition spec
  // (one global group) so the sort fallback drives the `SimpleLimitIterator` branch.
  private val attr = AttributeReference("a", IntegerType, nullable = false)()
  private val childOutput = Seq(attr)
  private val orderSpec = Seq(SortOrder(attr, Ascending))

  private def unsafeRows(values: Seq[Int]): Seq[UnsafeRow] = {
    val project = UnsafeProjection.create(Array[org.apache.spark.sql.types.DataType](IntegerType))
    values.map(v => project(InternalRow(v)).copy())
  }

  private def newFactory(
      limit: Int,
      fallbackMemoryThreshold: Long,
      metrics: Map[String, org.apache.spark.sql.execution.metric.SQLMetric])
    : HashWindowGroupLimitEvaluatorFactory = {
    new HashWindowGroupLimitEvaluatorFactory(
      partitionSpec = Nil,
      orderSpec = orderSpec,
      limit = limit,
      childOutput = childOutput,
      // Disable PASS_THROUGH: a sample size beyond the input means the ratio check never runs.
      passThroughRatio = 1.0,
      passThroughSampleRows = Int.MaxValue,
      fallbackMemoryThreshold = fallbackMemoryThreshold,
      enableRadixSort = true,
      numOutputRows = metrics("numOutputRows"),
      numFallbackTasks = metrics("numFallbackTasks"),
      numPassThroughTasks = metrics("numPassThroughTasks"),
      spillSize = metrics("spillSize"))
  }

  private def newMetrics() = Map(
    "numOutputRows" -> SQLMetrics.createMetric(spark.sparkContext, "output rows"),
    "numFallbackTasks" -> SQLMetrics.createMetric(spark.sparkContext, "fallback tasks"),
    "numPassThroughTasks" -> SQLMetrics.createMetric(spark.sparkContext, "pass-through tasks"),
    "spillSize" -> SQLMetrics.createSizeMetric(spark.sparkContext, "spill size"))

  private def withTaskContext(tmm: TaskMemoryManager)(body: => Unit): Unit = {
    val ctx = new TaskContextImpl(0, 0, 0, 0, 0, 1, tmm, new Properties, null)
    TaskContext.setTaskContext(ctx)
    try {
      body
    } finally {
      ctx.markTaskCompleted(None)
      TaskContext.unset()
    }
  }

  test("SPARK-58324: a spill request from the memory manager triggers SORT_FALLBACK") {
    // Small page so the memory numbers stay tiny and the forced-spill arithmetic is exact.
    val conf = new SparkConf(false).set(BUFFER_PAGESIZE.key, "1m")
    val memoryManager = new TestMemoryManager(conf)
    val pageSize = 1L * 1024 * 1024
    // Room for the buffer's first page plus headroom, but less than the co-tenant's request below,
    // so the co-tenant's acquire is short and the manager asks our consumer to spill.
    memoryManager.limit(8 * pageSize)
    val tmm = new TaskMemoryManager(memoryManager, 0)

    val metrics = newMetrics()
    val factory = newFactory(limit = 3, fallbackMemoryThreshold = Long.MaxValue, metrics)

    val values = Seq(5, 3, 8, 1, 9, 2, 7, 0, 6, 4, 15, 12, 11, 10, 14, 13, 19, 16, 18, 17)

    withTaskContext(tmm) {
      val coTenant = new TestMemoryConsumer(tmm)
      var seen = 0
      // While fetching the second row (so the first row is already buffered and the consumer holds
      // a page: getUsed > 0), have a same-mode co-tenant demand more memory than remains. The
      // manager cannot satisfy it, so it calls spill() on our consumer, which latches the request.
      // The co-tenant then frees, leaving room for the fallback sorter. The next loop iteration
      // (before the third row) sees the latched request and honors it.
      val input = new Iterator[InternalRow] {
        private val it = unsafeRows(values).iterator
        override def hasNext: Boolean = it.hasNext
        override def next(): InternalRow = {
          val row = it.next()
          seen += 1
          if (seen == 2) {
            coTenant.use(8 * pageSize)
            coTenant.free(coTenant.getUsed)
          }
          row
        }
      }

      val out = factory.createEvaluator().eval(0, input).map(_.asInstanceOf[UnsafeRow].getInt(0))
      // Empty partition spec + limit 3: the fallback sorts everything and keeps the 3 smallest.
      assert(out.toSeq == Seq(0, 1, 2))
    }

    assert(metrics("numFallbackTasks").value == 1,
      "the latched spill request must drive exactly one SORT_FALLBACK")
    assert(metrics("numPassThroughTasks").value == 0)
    assert(metrics("numOutputRows").value == 3)
  }

  test("SPARK-58324: no spill request keeps the task in the HASH path") {
    // A generous budget and no co-tenant: the consumer buffers everything and never falls back.
    val conf = new SparkConf(false).set(BUFFER_PAGESIZE.key, "1m")
    val memoryManager = new TestMemoryManager(conf)
    memoryManager.limit(64L * 1024 * 1024)
    val tmm = new TaskMemoryManager(memoryManager, 0)

    val metrics = newMetrics()
    val factory = newFactory(limit = 3, fallbackMemoryThreshold = Long.MaxValue, metrics)
    val values = Seq(5, 3, 8, 1, 9, 2, 7, 0, 6, 4)

    withTaskContext(tmm) {
      val input = unsafeRows(values).iterator.asInstanceOf[Iterator[InternalRow]]
      val out = factory.createEvaluator().eval(0, input).map(_.asInstanceOf[UnsafeRow].getInt(0))
      assert(out.toSeq.sorted == Seq(0, 1, 2))
    }

    assert(metrics("numFallbackTasks").value == 0)
    assert(metrics("numPassThroughTasks").value == 0)
    assert(metrics("numOutputRows").value == 3)
  }
}
