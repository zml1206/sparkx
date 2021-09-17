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
import org.apache.spark.sql.sparkcube.CubeSharedState
import org.apache.spark.sql.sparkcube.catalog.SourceType
import org.apache.spark.sql.sparkcube.conf.CubeConf

/**
 * Created by zhuml on 2021/8/3.
 */
case class ReplaceToDeltaRelation(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (sparkSession.sessionState.conf.getConf(CubeConf.D_USING_DELTA_TABLE)) {
      plan resolveOperators {
        case lr@LogicalRelation(jr@JDBCRelation(_, _, jdbcOptions), output, catalogTable, isStreaming) =>
          val cacheKey = CubeSharedState.cubeCatalog.cacheKey(
            SourceType.JDBC,
            getJdbcDb(jdbcOptions.url),
            jdbcOptions.tableOrQuery)
          val cacheTableInfoOption = CubeSharedState.cubeCatalog.cacheInfos.get(cacheKey)
          if (!cacheTableInfoOption.isEmpty) {
            val cacheInfo = cacheTableInfoOption.get
            if (StringUtils.isNotBlank(cacheInfo.deltaPath)) {
              val path = new Path(cacheInfo.deltaPath)
              val relation = DeltaTableV2(SparkSession.active, path).toBaseRelation
              LogicalRelation(relation, output, None, isStreaming)
            } else if (jr.jdbcOptions.numPartitions.isEmpty
              && cacheInfo.numPartitions > 1
              && !StringUtils.isAnyBlank(cacheInfo.partitionColumn, cacheInfo.lowerBound, cacheInfo.upperBound)) {
              val parameters = jr.jdbcOptions.parameters
                .++(Map(
                  "numPartitions" -> cacheInfo.numPartitions.toString,
                  "partitionColumn" -> cacheInfo.partitionColumn,
                  "lowerBound" -> cacheInfo.lowerBound,
                  "upperBound" -> cacheInfo.upperBound
                ))
              val baseRelation = new JdbcRelationProvider().createRelation(jr.sqlContext, parameters)
              LogicalRelation(baseRelation, output, catalogTable, isStreaming)
            } else {
              lr
            }
          } else {
            lr
          }
        case htr: HiveTableRelation =>
          val cacheKey = CubeSharedState.cubeCatalog.cacheKey(
            SourceType.JDBC,
            htr.tableMeta.identifier.database.get,
            htr.tableMeta.identifier.table)
          val cacheTableInfo = CubeSharedState.cubeCatalog.cacheInfos.get(cacheKey)
          if (!cacheTableInfo.isEmpty) {
            val path = new Path(cacheTableInfo.get.deltaPath)
            val relation = DeltaTableV2(SparkSession.active, path).toBaseRelation
            LogicalRelation(relation, htr.output, None, htr.isStreaming)
          } else {
            htr
          }
      }
    }
    else {
      plan
    }
  }

  def getJdbcDb(url: String): String = {
    url.split("\\?")(0).split("/").last
  }
}
