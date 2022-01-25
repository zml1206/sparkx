package org.apache.spark.sql.sparkcube.catalog

/**
 * Created by zhuml on 2021/8/9.
 */
class CubeApiExternalCatalog extends CubeExternalCatalog {

  override def getCacheInfos(): Map[String, CacheTableInfo] = {

    var cache = Map[String, CacheTableInfo]()
    // TODO: get cube meta by http
    cache
  }

}
