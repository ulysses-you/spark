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

import java.util.{HashMap => JHashMap, PriorityQueue => JPriorityQueue}

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, SparkEnv, TaskContext}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BindReferences, Expression, RowOrdering, SortOrder, SortPrefix, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{SortPrefixUtils, UnsafeExternalRowSorter}
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * Builds the evaluator for [[HashWindowGroupLimitExec]]. The evaluator keeps, per group, the
 * `limit` rows smallest by `orderSpec` in an in-memory bounded queue, so it does not need its
 * input to be sorted. It runs a three-state machine per task:
 *
 *   - HASH (default): maintain a per-group bounded queue; on input end, emit each group's rows.
 *   - PASS_THROUGH: re-checked every `passThroughSampleRows` input rows, if the cumulative
 *     surviving fraction exceeds `passThroughRatio`, the limit is ineffective, so stop filtering
 *     and pass the remaining rows through unchanged.
 *   - SORT_FALLBACK: buffered rows are tracked against the `TaskMemoryManager` -- each retained
 *     row acquires its accounted size (its byte size plus a fixed per-row object overhead) and
 *     each row evicted by the limit releases it. Fall back to a spillable external sorter, dumping
 *     the buffered rows plus the remaining input into it and applying the sort-based limit, when
 *     any of: the memory manager cannot grant the bytes for a new row (the task is under memory
 *     pressure); the buffered bytes exceed the optional `fallbackMemoryThreshold` cap; or the
 *     manager asks this operator to spill (another consumer needs the memory it holds). This
 *     bounds memory and keeps the operator a good citizen under contention.
 *
 * Dropping a row whose local position within a group already exceeds `limit` is always safe for
 * `RowNumber`: at least `limit` smaller rows of the same group are retained, so its global row
 * number must exceed `limit` and the final limit would drop it anyway.
 */
class HashWindowGroupLimitEvaluatorFactory(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    limit: Int,
    childOutput: Seq[Attribute],
    passThroughRatio: Double,
    passThroughSampleRows: Int,
    fallbackMemoryThreshold: Long,
    enableRadixSort: Boolean,
    numOutputRows: SQLMetric,
    numFallbackTasks: SQLMetric,
    numPassThroughTasks: SQLMetric,
    spillSize: SQLMetric)
  extends PartitionEvaluatorFactory[InternalRow, InternalRow] {

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new HashWindowGroupLimitPartitionEvaluator

  // The full sort order the sort-based fallback and the downstream window require.
  private def fullSortOrder: Seq[SortOrder] =
    partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec

  private def hashLimit(input: Iterator[InternalRow]): Iterator[InternalRow] = {
    val grouping = UnsafeProjection.create(partitionSpec, childOutput)
    val baseOrdering = RowOrdering.create(orderSpec, childOutput)
    // The bounded queue retains the "largest" rows under its ordering. To retain the rows that are
    // smallest by `orderSpec` (the ones with the smallest row number), reverse the ordering so the
    // queue evicts the row that is largest by `orderSpec`.
    val queueOrdering: Ordering[UnsafeRow] = new Ordering[UnsafeRow] {
      override def compare(x: UnsafeRow, y: UnsafeRow): Int = baseOrdering.compare(y, x)
    }

    // Track the bytes of the rows currently buffered against the task's memory manager. Each
    // buffered row acquires its size; each row evicted because the per-group limit is already full
    // releases its size. When the manager cannot grant the bytes for a new row (the task is under
    // memory pressure), or the buffered bytes exceed the configured cap, we fall back to the
    // spillable sort-based limit.
    val memoryConsumer = new RowBufferMemoryConsumer

    val buffers = new JHashMap[UnsafeRow, JPriorityQueue[UnsafeRow]]()
    var totalBuffered = 0L
    var totalInput = 0L

    // Drains the buffered rows, removing each queue and releasing its rows' and group's memory as
    // it goes so the reservation shrinks as rows leave rather than holding its peak until the end.
    // The reservation is held until rows are actually drained: on SORT_FALLBACK this iterator feeds
    // the external sorter and the buffered rows stay on the heap until the sorter copies them into
    // its own pages, so freeing up front would under-account the heap under the very memory
    // pressure that path exists to handle.
    def destructiveBufferedIterator: Iterator[InternalRow] = new Iterator[InternalRow] {
      private val entryIter = buffers.entrySet().iterator()
      private var current: JPriorityQueue[UnsafeRow] = null
      private var currentKey: UnsafeRow = null
      private var freed = false

      private def advance(): Unit = {
        while ((current == null || current.isEmpty) && entryIter.hasNext) {
          if (currentKey != null) {
            // The previous group is fully drained; return its key and per-group bytes.
            memoryConsumer.releaseGroupDrained(currentKey.getSizeInBytes)
            currentKey = null
          }
          val entry = entryIter.next()
          currentKey = entry.getKey
          current = entry.getValue
          entryIter.remove()
        }
      }

      override def hasNext: Boolean = {
        advance()
        val more = current != null && !current.isEmpty
        if (!more && !freed) {
          // Fully drained: release the sub-page remainder. If the consumer stops early instead, the
          // task-completion listener frees it.
          freed = true
          memoryConsumer.freeAll()
        }
        more
      }

      override def next(): InternalRow = {
        val row = if (current == null) null else current.poll()
        if (row == null) {
          throw new NoSuchElementException
        }
        memoryConsumer.releaseDrained(row.getSizeInBytes)
        row
      }
    }

    // Free all outstanding accounting when the task ends, in case the buffered rows are not fully
    // drained downstream (e.g. a downstream limit stops early). Idempotent with the drain above.
    TaskContext.get().addTaskCompletionListener[Unit](_ => memoryConsumer.freeAll())

    // Adds `row` to its group's bounded queue, evicting the group's largest-by-order row when the
    // queue is already full. Returns false if the memory needed to retain the row could not be
    // acquired -- the caller then triggers SORT_FALLBACK. `row` must be copied before it is
    // retained, because the input iterator reuses the same `UnsafeRow` instance across rows.
    def addRow(row: UnsafeRow): Boolean = {
      val group = grouping(row)
      var queue = buffers.get(group)
      if (queue == null) {
        // A new group retains a copy of its key and a bounded queue. Account for both first, so a
        // high-cardinality input trips SORT_FALLBACK rather than growing the map past what memory
        // allows. On failure nothing has been added yet, so the caller can fall back cleanly.
        if (!memoryConsumer.acquireGroup(group.getSizeInBytes)) {
          return false
        }
        // Default initial capacity rather than `limit`: `java.util.PriorityQueue` allocates its
        // array eagerly, so sizing to `limit` would pin an `Object[limit]` even for groups that
        // stay tiny -- the high-cardinality case this operator targets. Let it grow on demand.
        queue = new JPriorityQueue[UnsafeRow](queueOrdering)
        buffers.put(group.copy(), queue)
      }
      totalInput += 1
      if (queue.size < limit) {
        // The group is not full yet: retaining the row grows the buffer, so it needs memory.
        if (!memoryConsumer.acquire(row.getSizeInBytes)) {
          return false
        }
        queue.offer(row.copy())
        totalBuffered += 1
      } else {
        // The group is full: retain `row` only if it is smaller than the current largest, evicting
        // the largest. The buffered count is unchanged, so we release then re-acquire -- this is
        // where the limit "takes effect".
        val largest = queue.peek()
        if (queueOrdering.gt(row, largest)) {
          queue.poll()
          memoryConsumer.release(largest.getSizeInBytes)
          if (!memoryConsumer.acquire(row.getSizeInBytes)) {
            // `largest` is already evicted; retaining `row` would exceed the buffer's memory.
            return false
          }
          queue.offer(row.copy())
        }
      }
      true
    }

    while (input.hasNext) {
      if (memoryConsumer.spillRequested) {
        // The manager asked us to spill; `spill` could only latch the request (see there). Honor it
        // here by dumping the buffered rows plus the remaining input into the spillable sorter.
        numFallbackTasks += 1
        return sortFallback(destructiveBufferedIterator ++ input)
      }
      val row = input.next().asInstanceOf[UnsafeRow]
      if (!addRow(row)) {
        // SORT_FALLBACK: could not acquire memory for the buffers. Dump what we kept plus the
        // remaining input (including the row that did not fit) into the spillable sorter. The
        // buffers' accounting is NOT released here -- the buffered rows are still live and drained
        // lazily into the sorter, which holds the reservation until it frees it on exhaustion.
        numFallbackTasks += 1
        return sortFallback(destructiveBufferedIterator ++ Iterator.single(row) ++ input)
      }

      // Periodically re-evaluate whether the limit is filtering enough to be worth it, using the
      // cumulative survival ratio (`totalBuffered / totalInput`) so far. Re-checking rather than
      // deciding once means an unrepresentative early sample does not lock in the verdict.
      //
      // Limitation: on a highly compressible but very high-cardinality input, every group is still
      // filling during the sampled prefix, so the ratio reads close to 1.0 and this may pass
      // through even though the limit would eventually filter. Passing through is always correct --
      // the final limit still filters -- and only loses the pre-shuffle reduction. A
      // statistics-driven gate is left as a follow-up.
      if (totalInput % passThroughSampleRows == 0) {
        if (totalBuffered.toDouble / totalInput > passThroughRatio) {
          // PASS_THROUGH: the limit is not filtering enough to be worth it. Emit what we have kept
          // (already safely limited) and the remaining input unchanged.
          numPassThroughTasks += 1
          return (destructiveBufferedIterator ++ input).map { r =>
            numOutputRows += 1
            r
          }
        }
      }
    }

    // HASH mode completed: emit all buffered rows.
    destructiveBufferedIterator.map { r =>
      numOutputRows += 1
      r
    }
  }

  /**
   * A [[MemoryConsumer]] that accounts for the bytes the hash buffers pin, so they participate in
   * the task's memory budget. It drives SORT_FALLBACK from two directions: a short grant from
   * [[acquire]]/[[acquireGroup]] when it cannot reserve for a new row or group, and a [[spill]]
   * request from the manager, which it cannot satisfy synchronously and so latches for the main
   * loop to honor. Either way the caller switches to the external sorter, which can actually spill.
   *
   * Hardcoded [[MemoryMode.ON_HEAP]] (like [[org.apache.spark.util.collection.Spillable]]) because
   * the buffered rows are on-heap `byte[]` regardless of `spark.memory.offHeap.enabled`: the
   * on-heap execution pool always exists and `TaskMemoryManager.acquireExecutionMemory` routes by
   * the consumer's own mode, so this stays correctly bounded even under an off-heap deployment.
   *
   * To keep the per-row hot path off the synchronized [[org.apache.spark.memory.TaskMemoryManager]]
   * it reserves memory a page at a time: rows draw down a local pool refilled from the manager only
   * when it runs short, and an evicted row's bytes return to it without a manager round-trip.
   */
  private class RowBufferMemoryConsumer
    extends MemoryConsumer(
      TaskContext.get().taskMemoryManager(),
      TaskContext.get().taskMemoryManager().pageSizeBytes(),
      MemoryMode.ON_HEAP) {

    // Bytes granted by the manager but not yet charged to a buffered row. Most rows are charged
    // against this pool with no manager interaction; only a refill or the final release touches it.
    private var poolRemaining = 0L
    private val pageSize = TaskContext.get().taskMemoryManager().pageSizeBytes()

    // Fixed per-row overhead added to each row's payload `size`. Beyond the Tungsten payload that
    // `getSizeInBytes` returns, a retained row also carries a `UnsafeRow` wrapper (~40 bytes), the
    // `byte[]` header (~16 bytes), and its amortized slot in the group queue's backing array.
    // Over-accounting only trips the memory-safe SORT_FALLBACK sooner; under-accounting risks the
    // heap it protects. The group key is a retained copy too, so it carries this overhead as well.
    private val rowMemoryOverhead = 80L

    // Fixed per-group overhead, on top of the group key's own accounted bytes: the `JPriorityQueue`
    // object and its initial array, plus a `JHashMap` node and slot. The queue array grows on
    // demand (see `addRow`), so its per-row slots are charged to `rowMemoryOverhead`, not here.
    // Untracked, these let a high-cardinality input grow the buffers past what the manager knows we
    // hold and OOM without falling back.
    private val perGroupOverhead = 160L

    // Total bytes to reserve for a buffered row: the caller's payload `size` plus the overhead.
    private def accountedBytes(size: Long): Long = size + rowMemoryOverhead

    // Latched when the manager calls `spill` (see `spill`). Written from `spill` (on the task
    // thread, under the manager's lock) and read by the main loop; `volatile` for happens-before.
    @volatile private var spillRequestedFlag = false

    /** Whether the manager has asked this consumer to spill since the buffers started filling. */
    def spillRequested: Boolean = spillRequestedFlag

    /**
     * The manager asks this consumer to release memory. Our buffers are live JVM objects being
     * iterated by the caller, so dumping them to a sorter from inside `spill` would re-enter the
     * manager (the sorter acquires pages) while it is already inside this `acquireExecutionMemory`
     * call. Instead we free nothing now (return 0) but latch the request; the main loop sees
     * `spillRequested` at its next safe point and switches to the spillable SORT_FALLBACK path.
     */
    override def spill(size: Long, trigger: MemoryConsumer): Long = {
      spillRequestedFlag = true
      0L
    }

    /** Charges one buffered row: its payload `size` plus the per-row overhead. See [[reserve]]. */
    def acquire(size: Long): Boolean = reserve(accountedBytes(size))

    /**
     * Charges a newly created group: its retained key (payload `keySize` plus the per-row overhead,
     * since the key is a retained `UnsafeRow` copy) plus the per-group structural overhead.
     * Symmetric with [[releaseGroupDrained]]. See [[reserve]].
     */
    def acquireGroup(keySize: Long): Boolean = reserve(accountedBytes(keySize) + perGroupOverhead)

    /** Returns an evicted row's bytes to the local pool, without a manager round-trip. */
    def release(size: Long): Unit = poolRemaining += accountedBytes(size)

    /** Returns a drained row's bytes toward the manager. See [[releaseDrainedBytes]]. */
    def releaseDrained(size: Long): Unit = releaseDrainedBytes(accountedBytes(size))

    /** Returns a drained group's bytes (key plus per-group overhead) toward the manager. */
    def releaseGroupDrained(keySize: Long): Unit =
      releaseDrainedBytes(accountedBytes(keySize) + perGroupOverhead)

    /**
     * Reserves `bytes` against the local pool, refilling it from the manager a page at a time when
     * it runs short. Returns true on success; returns false without charging when the pool cannot
     * be refilled enough -- the task is out of memory, or the reservation would exceed
     * `fallbackMemoryThreshold`. A short refill grant is left in the pool and released together
     * with the rest by [[freeAll]].
     */
    private def reserve(bytes: Long): Boolean = {
      if (poolRemaining < bytes) {
        // Refill by at least a page, or `bytes` if that is larger than a page, without growing the
        // manager reservation past the configured cap.
        val request = math.max(pageSize, bytes - poolRemaining)
        if (getUsed + request > fallbackMemoryThreshold) {
          return false
        }
        poolRemaining += acquireMemory(request)
        if (poolRemaining < bytes) {
          return false
        }
      }
      poolRemaining -= bytes
      true
    }

    /**
     * Returns drained `bytes` toward the manager, a page at a time: they join the local pool, and
     * each full page in the pool is released. Unlike [[release]] (eviction, which re-acquires for
     * the replacement row and so keeps the bytes pooled), draining is terminal, so the reservation
     * shrinks as rows are emitted. This matters most on SORT_FALLBACK, where the reservation is
     * handed off to the external sorter row by row instead of both holding their peak at once.
     */
    private def releaseDrainedBytes(bytes: Long): Unit = {
      poolRemaining += bytes
      while (poolRemaining >= pageSize) {
        freeMemory(pageSize)
        poolRemaining -= pageSize
      }
    }

    /** Releases the whole outstanding reservation back to the manager. Idempotent. */
    def freeAll(): Unit = {
      val used = getUsed
      if (used > 0) {
        freeMemory(used)
      }
      poolRemaining = 0L
    }
  }


  /**
   * Sorts `input` by `fullSortOrder` using a spillable external sorter, then applies the
   * sort-based limit. The sorter is built with the same prefix-comparator and radix-sort setup as
   * `SortExec` so the fallback sorts rows exactly as the sort-based partial would.
   */
  private def sortFallback(input: Iterator[InternalRow]): Iterator[InternalRow] = {
    val schema = DataTypeUtils.fromAttributes(childOutput)
    val ordering = RowOrdering.create(fullSortOrder, childOutput)

    // The comparator for comparing prefix.
    val boundSortExpression = BindReferences.bindReference(fullSortOrder.head, childOutput)
    val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)
    val canUseRadixSort = enableRadixSort && fullSortOrder.length == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)

    // The generator for prefix.
    val prefixExpr = SortPrefix(boundSortExpression)
    val prefixProjection = UnsafeProjection.create(Seq(prefixExpr))
    val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
      private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
      override def computePrefix(row: InternalRow):
          UnsafeExternalRowSorter.PrefixComputer.Prefix = {
        val prefix = prefixProjection.apply(row)
        result.isNull = prefix.isNullAt(0)
        result.value = if (result.isNull) prefixExpr.nullValue else prefix.getLong(0)
        result
      }
    }

    val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
    val sorter = UnsafeExternalRowSorter.create(
      schema, ordering, prefixComparator, prefixComputer, pageSize, canUseRadixSort)

    val taskContext = TaskContext.get()
    val spillSizeBefore = taskContext.taskMetrics().memoryBytesSpilled
    val sortedIterator = sorter.sort(input.asInstanceOf[Iterator[UnsafeRow]])
    spillSize += taskContext.taskMetrics().memoryBytesSpilled - spillSizeBefore

    val limitFunc = (iter: Iterator[InternalRow]) => SimpleLimitIterator(iter, limit, numOutputRows)
    if (partitionSpec.isEmpty) {
      limitFunc(sortedIterator)
    } else {
      new GroupedLimitIterator(sortedIterator, childOutput, partitionSpec, limitFunc)
    }
  }

  class HashWindowGroupLimitPartitionEvaluator
    extends PartitionEvaluator[InternalRow, InternalRow] {

    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      hashLimit(inputs.head)
    }
  }
}
