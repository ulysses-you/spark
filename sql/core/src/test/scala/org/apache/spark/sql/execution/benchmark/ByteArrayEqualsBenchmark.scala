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

import scala.util.Random

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String

/**
 * Benchmark to measure performance for byte array comparisons.
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/<this class>-results.txt".
 * }}}
 */
object ByteArrayEqualsBenchmark extends BenchmarkBase {

  def byteArrayEquals(iters: Long): Unit = {
    val chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    val random = new Random(0)
    def randomBytes(min: Int, max: Int): Array[Byte] = {
      val len = random.nextInt(max - min) + min
      val bytes = new Array[Byte](len)
      var i = 0
      while (i < len) {
        bytes(i) = chars.charAt(random.nextInt(chars.length())).toByte
        i += 1
      }
      bytes
    }

    def binaryEquals(inputs: Array[BinaryEqualInfo], fast: Boolean) = { _: Int =>
      var res = false
      for (_ <- 0L until iters) {
        inputs.foreach { input =>
          res = if (fast) {
            ByteArrayMethods.arrayEqualsFast(
              input.s1.getBaseObject, input.s1.getBaseOffset,
              input.s2.getBaseObject, input.s2.getBaseOffset + input.deltaOffset,
              input.len)
          } else {
            ByteArrayMethods.arrayEquals(
              input.s1.getBaseObject, input.s1.getBaseOffset,
              input.s2.getBaseObject, input.s2.getBaseOffset + input.deltaOffset,
              input.len)
          }
        }
      }
    }
    val count = 16 * 1000
    val rand = new Random(0)
    val inputs = (0 until count).map { _ =>
      val s1 = UTF8String.fromBytes(randomBytes(1, 16))
      val s2 = UTF8String.fromBytes(randomBytes(1, 16))
      val len = s1.numBytes().min(s2.numBytes())
      val deltaOffset = rand.nextInt(len)
      BinaryEqualInfo(s1, s2, deltaOffset, len)
    }.toArray

    val benchmark = new Benchmark("Byte Array equals", count * iters, 25, output = output)
    benchmark.addCase("Byte Array equals fast")(binaryEquals(inputs, true))
    benchmark.addCase("Byte Array equals")(binaryEquals(inputs, false))
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("byte array equals") {
      byteArrayEquals(1000 * 10)
    }
  }

  case class BinaryEqualInfo(
      s1: UTF8String,
      s2: UTF8String,
      deltaOffset: Int,
      len: Int)
}
