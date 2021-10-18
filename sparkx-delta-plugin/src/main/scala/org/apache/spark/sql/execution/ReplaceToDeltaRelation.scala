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
import org.apache.spark.sql.sparkcube.conf.CubeConf

/**
 * Created by zhuml on 2021/8/3.
 */
case class ReplaceToDeltaRelation(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  private def usingDeltaTable(): Boolean = {
    sparkSession.sessionState.conf.getConf(CubeConf.D_USING_DELTA_TABLE)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case lr@LogicalRelation(jr@JDBCRelation(_, _, jdbcOptions), output, catalogTable, isStreaming)
        if usingDeltaTable =>
        val cacheKey = CubeSharedState.cubeCatalog.cacheJDBCKey(jdbcOptions)
        val cacheTableInfoOption = CubeSharedState.cubeCatalog.cacheInfos.get(cacheKey)
        if (cacheTableInfoOption.nonEmpty) {
          val cacheInfo = cacheTableInfoOption.get
          if (Option(cacheInfo.deltaPath).nonEmpty) {
            val relation = DeltaTableV2(SparkSession.active, new Path(cacheInfo.deltaPath)).toBaseRelation
            return LogicalRelation(relation, output, None, isStreaming)
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
            return LogicalRelation(baseRelation, output, catalogTable, isStreaming)
          }
        }
        lr
      case htr: HiveTableRelation if usingDeltaTable =>
        val cacheKey = CubeSharedState.cubeCatalog.cacheHiveKey(htr)
        val cacheTableInfo = CubeSharedState.cubeCatalog.cacheInfos.get(cacheKey)
        if (cacheTableInfo.nonEmpty && Option(cacheTableInfo.get.deltaPath).nonEmpty) {
          val relation = DeltaTableV2(SparkSession.active, new Path(cacheTableInfo.get.deltaPath)).toBaseRelation
          LogicalRelation(relation, htr.output, None, htr.isStreaming)
        } else {
          htr
        }
    }
  }

  def getJdbcDb(url: String): String = {
    url.split("\\?")(0).split("/").last
  }
}
