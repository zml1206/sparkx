package org.apache.spark.sql.sparkcube.catalog

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.sparkcube.CubeSharedState

/**
 * Created by zhuml on 2021/11/5.
 */
object HiveMatch {
  def unapply(tableMeta: CatalogTable): Option[CacheTableInfo] = {
    val cacheKey = CubeSharedState.cubeCatalog.cacheHiveKey(tableMeta)
    CubeSharedState.cubeCatalog.cacheInfos.get(cacheKey)
  }
}

object JdbcMatch {
  def unapply(jdbcOptions: JDBCOptions): Option[CacheTableInfo] = {
    val cacheKey = CubeSharedState.cubeCatalog.cacheJDBCKey(jdbcOptions)
    CubeSharedState.cubeCatalog.cacheInfos.get(cacheKey)
  }
}
