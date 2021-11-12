package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sparkcube.catalog.HiveMatch
import org.apache.spark.sql.sparkcube.conf.CubeConf

/**
 * Created by zhuml on 2021/10/12.
 */
case class HiveRefreshRelation(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (sparkSession.sessionState.conf.getConf(CubeConf.D_USING_HIVE_REFRESH)) genPlan(plan) else plan
  }

  def genPlan(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case hr @ HiveTableRelation(tableMeta @ HiveMatch(cacheInfo), _, _, _, _)
        if Option(cacheInfo.format).nonEmpty
          && Array("parquet", "orc").contains(cacheInfo.format) =>
        sparkSession.sessionState.catalog.refreshTable(tableMeta.identifier)
        hr
    }
  }
}
