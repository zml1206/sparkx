package org.apache.spark.sql.execution

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sparkcube.conf.CubeConf

/**
 * Created by zhuml on 2021/11/18.
 */
case class HivePartitionFilterCheck(sparkSession: SparkSession) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (sparkSession.sessionState.conf.getConf(CubeConf.D_USING_HIVE_PARTITION_CHECK)) check(plan) else Nil
  }

  def check(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ScanOperation(_, _, h: HiveTableRelation) if h.isPartitioned && h.prunedPartitions == None =>
      throw new PartitionFilterNotExistException(h.tableMeta.identifier)
    case ScanOperation(_, f, HiveLogicalRelation(ident, partitions))
      if (f.flatMap(_.references).map(_.name) intersect partitions).size == 0 =>
      throw new PartitionFilterNotExistException(ident)
    case _ => Nil
  }
}

object HiveLogicalRelation {
  type ReturnType = (TableIdentifier, Seq[String])

  def unapply(relation: LogicalRelation): Option[ReturnType] = relation.catalogTable match {
    case Some(table) if table.provider == Some("hive") && table.partitionColumnNames.nonEmpty =>
      Option(table.identifier, table.partitionColumnNames)
    case _ => None
  }
}
