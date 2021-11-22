package org.apache.spark.sql.execution

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.ZIndexCacheExtensions.ExtensionsBuilder

/**
 * Created by zhuml on 2021/11/18.
 */
class CacheExtensions extends ExtensionsBuilder with Logging {
  override def apply(sessionExtensions: SparkSessionExtensions): Unit = {
    logInfo("register extension CacheExtensions.")
    sessionExtensions.injectResolutionRule(s => ReplaceToDeltaRelation(s))
    sessionExtensions.injectResolutionRule(s => HiveRefreshRelation(s))
    sessionExtensions.injectPlannerStrategy(s => HivePartitionFilterCheck(s))
  }
}

object ZIndexCacheExtensions {
  type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
  type ExtensionsBuilder = SparkSessionExtensions => Unit
}
