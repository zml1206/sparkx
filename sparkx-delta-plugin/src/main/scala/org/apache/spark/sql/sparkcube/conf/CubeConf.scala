package org.apache.spark.sql.sparkcube.conf

import org.apache.spark.sql.internal.SQLConf

/**
 * Created by zhuml on 2021/8/9.
 */
object CubeConf {

  import SQLConf.buildStaticConf

  val D_USING_DELTA_TABLE =
    buildStaticConf("spark.d.using.delta.table")
      .internal()
      .doc("")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val D_USING_HIVE_REFRESH =
    buildStaticConf("spark.d.using.hive.refresh")
      .internal()
      .doc("")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val D_USING_HIVE_PARTITION_CHECK =
    buildStaticConf("spark.d.check.hive.partition")
      .internal()
      .doc("")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val CUBE_CATALOG_IMPLEMENTATION = buildStaticConf("spark.sql.cube.catalogImplementation")
    .internal()
    .stringConf
    .checkValues(Set("api"))
    .createWithDefault("api")
}
