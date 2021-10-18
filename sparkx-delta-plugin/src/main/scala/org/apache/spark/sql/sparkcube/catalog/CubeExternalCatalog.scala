package org.apache.spark.sql.sparkcube.catalog

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

/**
 * Created by zhuml on 2021/8/9.
 */
abstract class CubeExternalCatalog extends Logging {

  var cacheInfos = getCacheInfos()

  def getCacheInfos(): Map[String, CacheTableInfo]

  def updateCacheInfos(): Unit = {
    logInfo("Update SparkCube Metadata Cache")
    cacheInfos = getCacheInfos()
  }

  def cacheKey(sourceType: String, db: String, table: String): String = {
    s"$sourceType:$db:$table"
  }

  def cacheKey(sourceType: SourceType.Value, db: String, table: String): String = {
    cacheKey(sourceType.toString, db, table)
  }

  def cacheHiveKey(htr: HiveTableRelation): String = {
    cacheKey(
      SourceType.Hive,
      htr.tableMeta.identifier.database.get,
      htr.tableMeta.identifier.table)
  }

  def getJdbcDb(url: String): String = {
    url.split("\\?")(0).split("/").last
  }

  def cacheJDBCKey(jdbcOptions: JDBCOptions): String = {
    cacheKey(
      SourceType.JDBC,
      getJdbcDb(jdbcOptions.url),
      jdbcOptions.tableOrQuery)
  }
}

object SourceType extends Enumeration {
  val JDBC = Value("jdbc")
  val Hive = Value("hive")
}



