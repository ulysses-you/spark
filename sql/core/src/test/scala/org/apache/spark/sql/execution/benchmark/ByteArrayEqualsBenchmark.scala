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

/**
 * Benchmark to measure performance for byte array operators.
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
  private val chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  private val randomChar = new Random(0)

  def randomBytes(len: Int): Array[Byte] = {
    val bytes = new Array[Byte](len)
    var i = 0
    while (i < len) {
      bytes(i) = chars.charAt(randomChar.nextInt(chars.length())).toByte
      i += 1
    }
    bytes
  }

  def byteArrayEquals(iters: Long): Unit = {
    val count = 16 * 1000
    def binaryEquals(data: Array[Array[Byte]], unsafe: Boolean) = { _: Int =>
      var res = false
      for (_ <- 0L until iters) {
        var i = 0
        while (i < count) {
          res = if (unsafe) {
            ByteArrayMethods.arrayEquals(data(i), data((i + 1) % count))
          } else {
            java.util.Arrays.equals(data(i), data((i + 1) % count))
          }
          i += 1
        }
      }
    }

    val data2 = Seq.fill(count)(randomBytes(2)).toArray
    val data4 = Seq.fill(count)(randomBytes(4)).toArray
    val data8 = Seq.fill(count)(randomBytes(8)).toArray
    val data12 = Seq.fill(count)(randomBytes(12)).toArray
    val data16 = Seq.fill(count)(randomBytes(16)).toArray
    val data32 = Seq.fill(count)(randomBytes(16)).toArray
    val data64 = Seq.fill(count)(randomBytes(64)).toArray
    val data128 = Seq.fill(count)(randomBytes(128)).toArray
    val data512Slow = Seq.fill(count)(
      Array.tabulate(128) {i => if (i < 127) 0.toByte else 1.toByte}).toArray
    val benchmark = new Benchmark("Byte Array equals off heap", count * iters, 25, output = output)
    benchmark.addCase("byte array size 2")(binaryEquals(data2, true))
    benchmark.addCase("byte array size 4")(binaryEquals(data4, true))
    benchmark.addCase("byte array size 8")(binaryEquals(data8, true))
    benchmark.addCase("byte array size 12")(binaryEquals(data12, true))
    benchmark.addCase("byte array size 16")(binaryEquals(data16, true))
    benchmark.addCase("byte array size 32")(binaryEquals(data32, true))
    benchmark.addCase("byte array size 64")(binaryEquals(data64, true))
    benchmark.addCase("byte array size 128")(binaryEquals(data128, true))
    benchmark.addCase("byte array size 128 slow")(binaryEquals(data512Slow, true))
    benchmark.run()

    val benchmark2 = new Benchmark("Byte Array equals on heap", count * iters, 25, output = output)
    benchmark2.addCase("byte array size 2")(binaryEquals(data2, false))
    benchmark2.addCase("byte array size 4")(binaryEquals(data4, false))
    benchmark2.addCase("byte array size 8")(binaryEquals(data8, false))
    benchmark2.addCase("byte array size 12")(binaryEquals(data12, false))
    benchmark2.addCase("byte array size 16")(binaryEquals(data16, false))
    benchmark2.addCase("byte array size 32")(binaryEquals(data32, false))
    benchmark2.addCase("byte array size 64")(binaryEquals(data64, false))
    benchmark2.addCase("byte array size 128")(binaryEquals(data128, false))
    benchmark2.addCase("byte array size 128 slow")(binaryEquals(data512Slow, false))
    benchmark2.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("byte array equals") {
      byteArrayEquals(1000 * 10)
    }
  }
}
