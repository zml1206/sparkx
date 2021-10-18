package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sparkcube.CubeSharedState
import org.apache.spark.sql.sparkcube.conf.CubeConf

/**
 * Created by zhuml on 2021/10/12.
 */
case class HiveRefreshRelation(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  private def usingRefresh(): Boolean = {
    sparkSession.sessionState.conf.getConf(CubeConf.D_USING_HIVE_REFRESH)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case htr: HiveTableRelation if usingRefresh =>
        val cacheKey = CubeSharedState.cubeCatalog.cacheHiveKey(htr)
        val cacheTableInfo = CubeSharedState.cubeCatalog.cacheInfos.get(cacheKey)
        if (cacheTableInfo.nonEmpty
          && Option(cacheTableInfo.get.format).nonEmpty
          && Array("parquet", "orc").contains(cacheTableInfo.get.format)) {
          sparkSession.sessionState.catalog.refreshTable(htr.tableMeta.identifier)
        }
        htr
    }
  }
}
