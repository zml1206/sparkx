package org.apache.spark.sql.sparkcube

import org.apache.spark.SparkContext
import org.apache.spark.sql.sparkcube.catalog.{CubeApiExternalCatalog, CubeExternalCatalog}
import org.apache.spark.sql.sparkcube.conf.CubeConf.CUBE_CATALOG_IMPLEMENTATION

/**
 * Created by zhuml on 2021/8/9.
 */

object CubeSharedState {

  val cubeCatalog: CubeExternalCatalog = {
    SparkContext.getOrCreate().getConf.get(CUBE_CATALOG_IMPLEMENTATION) match {
      case "api" => new CubeApiExternalCatalog()
    }
  }
}
