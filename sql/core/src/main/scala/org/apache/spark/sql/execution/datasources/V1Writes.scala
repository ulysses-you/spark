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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, BitwiseAnd, HiveHash, Literal, Pmod, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.internal.SQLConf

/**
 * A rule that constructs logical writes for datasource v1.
 */
object V1Writes extends Rule[LogicalPlan] with V1WritesHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case i @ InsertIntoHadoopFsRelationCommand(_, staticPartitions, _, partitionColumns,
      bucketSpec, _, options, query, _, _, _, _, false) =>
      i.copy(
        query =
          prepareQuery(
            query,
            i.outputColumns,
            partitionColumns,
            partitionColumns.take(staticPartitions.size),
            bucketSpec,
            options),
        outputOrderResolved = true)

    case c @ CreateDataSourceTableAsSelectCommand(table, _, query, _, false) =>
      val partitionColumns = table.partitionColumnNames.map { name =>
        query.resolve(name :: Nil, SparkSession.active.sessionState.analyzer.resolver).getOrElse {
          throw QueryCompilationErrors.cannotResolveAttributeError(
            name, query.output.map(_.name).mkString(", "))
        }.asInstanceOf[Attribute]
      }
      c.copy(
        query =
          prepareQuery(
            query,
            c.outputColumns,
            partitionColumns,
            Seq.empty,
            table.bucketSpec,
            table.storage.properties
          ),
        outputOrderResolved = true)

    case _ => plan
  }
}

trait V1WritesHelper {

  def getBucketSpec(
      bucketSpec: Option[BucketSpec],
      dataColumns: Seq[Attribute],
      options: Map[String, String]): Option[WriterBucketSpec] = {
    bucketSpec.map { spec =>
      val bucketColumns = spec.bucketColumnNames.map(c => dataColumns.find(_.name == c).get)
      if (options.getOrElse(BucketingUtils.optionForHiveCompatibleBucketWrite, "false") ==
        "true") {
        // Hive bucketed table: use `HiveHash` and bitwise-and as bucket id expression.
        // Without the extra bitwise-and operation, we can get wrong bucket id when hash value of
        // columns is negative. See Hive implementation in
        // `org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils#getBucketNumber()`.
        val hashId = BitwiseAnd(HiveHash(bucketColumns), Literal(Int.MaxValue))
        val bucketIdExpression = Pmod(hashId, Literal(spec.numBuckets))

        // The bucket file name prefix is following Hive, Presto and Trino conversion, so this
        // makes sure Hive bucketed table written by Spark, can be read by other SQL engines.
        //
        // Hive: `org.apache.hadoop.hive.ql.exec.Utilities#getBucketIdFromFile()`.
        // Trino: `io.trino.plugin.hive.BackgroundHiveSplitLoader#BUCKET_PATTERNS`.
        val fileNamePrefix = (bucketId: Int) => f"$bucketId%05d_0_"
        WriterBucketSpec(bucketIdExpression, fileNamePrefix)
      } else {
        // Spark bucketed table: use `HashPartitioning.partitionIdExpression` as bucket id
        // expression, so that we can guarantee the data distribution is same between shuffle and
        // bucketed data source, which enables us to only shuffle one side when join a bucketed
        // table and a normal one.
        val bucketIdExpression = HashPartitioning(bucketColumns, spec.numBuckets)
          .partitionIdExpression
        WriterBucketSpec(bucketIdExpression, (_: Int) => "")
      }
    }
  }

  def getBucketSortColumns(
      bucketSpec: Option[BucketSpec], dataColumns: Seq[Attribute]): Seq[Attribute] = {
    bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
    }
  }

  def getSortOrder(
      outputColumns: Seq[Attribute],
      partitionColumns: Seq[Attribute],
      staticPartitions: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String]): Seq[SortOrder] = {
    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = outputColumns.filterNot(partitionSet.contains)
    val writerBucketSpec = getBucketSpec(bucketSpec, dataColumns, options)
    val sortColumns = getBucketSortColumns(bucketSpec, dataColumns)

    assert(partitionColumns.size >= staticPartitions.size)
    // We should first sort by partition columns, then bucket id, and finally sorting columns.
    (partitionColumns.take(partitionColumns.size - staticPartitions.size) ++
      writerBucketSpec.map(_.bucketIdExpression) ++ sortColumns)
      .map(SortOrder(_, Ascending))
  }

  def prepareQuery(
      query: LogicalPlan,
      outputColumns: Seq[Attribute],
      partitionColumns: Seq[Attribute],
      staticPartitions: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String]): LogicalPlan = {
    val requiredOrdering = getSortOrder(
      outputColumns, partitionColumns, staticPartitions, bucketSpec, options)
    val actualOrdering = query.outputOrdering
    val orderingMatched = if (requiredOrdering.length > actualOrdering.length) {
      false
    } else {
      requiredOrdering.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) =>
          requiredOrder.semanticEquals(childOutputOrder)
      }
    }

    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = outputColumns.filterNot(partitionSet.contains)
    val sortColumns = getBucketSortColumns(bucketSpec, dataColumns)
    if (orderingMatched ||
        (SQLConf.get.maxConcurrentOutputFileWriters > 0 && sortColumns.isEmpty)) {
      query
    } else {
      Sort(requiredOrdering, false, query)
    }
  }
}
