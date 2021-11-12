package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sparkcube.conf.CubeConf

/**
 * Created by zhuml on 2021/11/1.
 */
case class HivePartitionFilterCheckRelation(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (sparkSession.sessionState.conf.getConf(CubeConf.D_USING_HIVE_PARTITION_CHECK)) check(plan) else plan
  }

  def check(plan: LogicalPlan): LogicalPlan =
    plan resolveOperators {
      case Project(_, HivePartitionTableLike(ident, _)) =>
        throw new PartitionFilterNotExistException(ident)
      case Filter(condition, HivePartitionTableLike(ident, partitionCols))
        if condition.flatMap(_.references).map(_.name).intersect(partitionCols).isEmpty =>
        throw new PartitionFilterNotExistException(ident)
      case Aggregate(_, _, HivePartitionTableLike(ident, _)) =>
        throw new PartitionFilterNotExistException(ident)
      case Join(HivePartitionTableLike(ident, _), _, _, _, _) =>
        throw new PartitionFilterNotExistException(ident)
      case Join(_, HivePartitionTableLike(ident, _), _, _, _) =>
        throw new PartitionFilterNotExistException(ident)
      case LocalLimit(_, HivePartitionTableLike(ident, _)) =>
        throw new PartitionFilterNotExistException(ident)
    }
}

object HivePartitionTableLike {
  def unapply(plan: LogicalPlan): Option[(TableIdentifier, Seq[String])] = plan match {
    case HiveTableRelation(tableMeta, _, partitionCols, _, _) if partitionCols.nonEmpty =>
      Some(tableMeta.identifier, partitionCols.map(_.name))
    case LogicalRelation(_, _, CatalogTableLike(table), _) =>
      Option(table)
    case _ => None
  }
}

object CatalogTableLike {
  type ReturnType = (TableIdentifier, Seq[String])

  def unapply(catalogTable: Option[CatalogTable]): Option[ReturnType] = catalogTable match {
    case Some(table) if table.provider == Some("hive") && table.partitionColumnNames.nonEmpty =>
      Option(table.identifier, table.partitionColumnNames)
    case _ => None
  }
}
