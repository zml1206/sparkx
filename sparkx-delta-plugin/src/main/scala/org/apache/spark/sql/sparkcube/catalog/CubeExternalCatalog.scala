package org.apache.spark.sql.sparkcube.catalog

import org.apache.spark.internal.Logging

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
}

object SourceType extends Enumeration {
  val JDBC = Value("jdbc")
  val Hive = Value("hive")
}



