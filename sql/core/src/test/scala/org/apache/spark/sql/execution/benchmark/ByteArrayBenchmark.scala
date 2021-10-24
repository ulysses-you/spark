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
import org.apache.spark.unsafe.types.ByteArray

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
object ByteArrayBenchmark extends BenchmarkBase {
  def compareBinaryOld(x: Array[Byte], y: Array[Byte]): Int = {
    val limit = if (x.length <= y.length) x.length else y.length
    var i = 0
    while (i < limit) {
      val res = (x(i) & 0xff) - (y(i) & 0xff)
      if (res != 0) return res
      i += 1
    }
    x.length - y.length
  }

  def byteArrayComparisons(iters: Long): Unit = {
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

    val count = 16 * 1000
    val dataTiny = Seq.fill(count)(randomBytes(2, 7)).toArray
    val dataSmall = Seq.fill(count)(randomBytes(8, 16)).toArray
    val dataMedium = Seq.fill(count)(randomBytes(16, 32)).toArray
    val dataLarge = Seq.fill(count)(randomBytes(512, 1024)).toArray
    val dataLargeSlow = Seq.fill(count)(
      Array.tabulate(512) {i => if (i < 511) 0.toByte else 1.toByte}).toArray

    def compareBinary(
        data: Array[Array[Byte]], compareFunc: (Array[Byte], Array[Byte]) => Int) = { _: Int =>
      var sum = 0L
      for (_ <- 0L until iters) {
        var i = 0
        while (i < count) {
          sum += compareFunc(data(i), data((i + 1) % count))
          i += 1
        }
      }
    }

    val benchmark = new Benchmark("Byte Array compare offHeap", count * iters, 25, output = output)
    benchmark.addCase("2-7 byte")(compareBinary(dataTiny, ByteArray.compareBinary))
    benchmark.addCase("8-16 byte")(compareBinary(dataSmall, ByteArray.compareBinary))
    benchmark.addCase("16-32 byte")(compareBinary(dataMedium, ByteArray.compareBinary))
    benchmark.addCase("512-1024 byte")(compareBinary(dataLarge, ByteArray.compareBinary))
    benchmark.addCase("512 byte slow")(compareBinary(dataLargeSlow, ByteArray.compareBinary))
    benchmark.addCase("2-7 byte")(compareBinary(dataTiny, ByteArray.compareBinary))
    benchmark.run()

    val benchmark2 = new Benchmark("Byte Array compare onHeap", count * iters, 25, output = output)
    benchmark2.addCase("2-7 byte")(compareBinary(dataTiny, compareBinaryOld))
    benchmark2.addCase("8-16 byte")(compareBinary(dataSmall, compareBinaryOld))
    benchmark2.addCase("16-32 byte")(compareBinary(dataMedium, compareBinaryOld))
    benchmark2.addCase("512-1024 byte")(compareBinary(dataLarge, compareBinaryOld))
    benchmark2.addCase("512 byte slow")(compareBinary(dataLargeSlow, compareBinaryOld))
    benchmark2.addCase("2-7 byte")(compareBinary(dataTiny, compareBinaryOld))
    benchmark2.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("byte array comparisons") {
      byteArrayComparisons(1024 * 4)
    }
  }
}
