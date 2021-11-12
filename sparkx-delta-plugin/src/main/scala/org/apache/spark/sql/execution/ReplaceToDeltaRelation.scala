package org.apache.spark.sql.execution

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCRelation, JdbcRelationProvider}
import org.apache.spark.sql.sparkcube.catalog.{CacheTableInfo, HiveMatch, JdbcMatch}
import org.apache.spark.sql.sparkcube.conf.CubeConf

/**
 * Created by zhuml on 2021/8/3.
 */
case class ReplaceToDeltaRelation(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (sparkSession.sessionState.conf.getConf(CubeConf.D_USING_DELTA_TABLE)) genPlan(plan) else plan
  }

  def genPlan(plan: LogicalPlan): LogicalPlan =
    plan resolveOperators {
      case lr @ LogicalRelation(jr@JDBCRelation(_, _, jo @ JdbcMatch(cacheInfo)), output, catalogTable, isStreaming) =>
        if (Option(cacheInfo.deltaPath).nonEmpty) {
          val relation = DeltaTableV2(SparkSession.active, new Path(cacheInfo.deltaPath)).toBaseRelation
          return LogicalRelation(relation, output, None, isStreaming)
        } else if (jo.numPartitions.isEmpty && jdbcIsPartition(cacheInfo)) {
          val parameters = jo.parameters
            .++(Map(
              "numPartitions" -> cacheInfo.numPartitions.toString,
              "partitionColumn" -> cacheInfo.partitionColumn,
              "lowerBound" -> cacheInfo.lowerBound,
              "upperBound" -> cacheInfo.upperBound
            ))
          val baseRelation = new JdbcRelationProvider().createRelation(jr.sqlContext, parameters)
          return LogicalRelation(baseRelation, output, catalogTable, isStreaming)
        } else {
          lr
        }
      case htr @ HiveTableRelation(HiveMatch(cacheInfo), _, _, _, _) if Option(cacheInfo.deltaPath).nonEmpty =>
        val relation = DeltaTableV2(SparkSession.active, new Path(cacheInfo.deltaPath)).toBaseRelation
        LogicalRelation(relation, htr.output, None, htr.isStreaming)
    }

  def jdbcIsPartition(cacheInfo: CacheTableInfo): Boolean = {
    if (cacheInfo.numPartitions > 1
      && !StringUtils.isAnyBlank(cacheInfo.partitionColumn, cacheInfo.lowerBound, cacheInfo.upperBound)) {
      true
    } else {
      false
    }
  }

  def getJdbcDb(url: String): String = {
    url.split("\\?")(0).split("/").last
  }
}
