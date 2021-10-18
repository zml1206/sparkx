package org.apache.spark.sql.sparkcube.catalog

/**
 * Created by zhuml on 2021/8/9.
 */
case class CacheTableInfo(sourceType: String,
                          db: String,
                          table: String,
                          deltaPath: String,
                          partitionColumn: String,
                          numPartitions: Int,
                          lowerBound: String,
                          upperBound: String,
                          isView: Boolean,
                          format: String)
