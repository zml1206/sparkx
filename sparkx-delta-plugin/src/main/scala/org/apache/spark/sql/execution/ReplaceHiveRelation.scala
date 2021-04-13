package org.apache.spark.sql.execution

import scala.collection.immutable.HashMap

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf

/**
 * @time 2021/4/13 11:04 上午
 * @author fchen <cloud.chenfu@gmail.com>
 */
case class ReplaceHiveRelation(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    //    plan transform {
    //
    //      case p @ PhysicalOperation(fields, filters, t @ HiveTableRelation(tableMeta, _, _, _, _)) =>
    //        val path = new Path("/tmp/delta/src")
    //        DeltaTableV2(null, path).toBaseRelation
    //    }
    if (sparkSession.sessionState.conf.getConf(ReplaceHiveRelation.D_USING_DELTA_TABLE)) {
      plan resolveOperators {
        case htr: HiveTableRelation
          if ReplaceHiveRelation.DELTA_TABLE_SET.contains(htr.tableMeta.identifier.toString) =>
          val path = new Path(ReplaceHiveRelation.DELTA_TABLE_SET(htr.tableMeta.identifier.toString()))
          val relation = DeltaTableV2(SparkSession.active, path).toBaseRelation
          LogicalRelation(relation, htr.output, None, htr.isStreaming)
      }
    } else {
      plan
    }
  }
}

object ReplaceHiveRelation {

  // the hive table to delta path map info.
  val DELTA_TABLE_SET = HashMap(
    "`bdl_db_chd`.`dxmm_app_event_di`" -> "/public/delta/bdl_db_chd/dxmm_app_event_di"
  )

  val D_USING_DELTA_TABLE =
    SQLConf.buildConf("spark.d.using.delta.table")
      .internal()
      .doc("")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

//  val HELLO =
//    SQLConf.buildConf("spark.sql.hello.conf")
//      .internal()
//      .doc("")
//      .version("3.0.0")
//      .stringConf
//      .createWithDefault("nihaoya")
}
