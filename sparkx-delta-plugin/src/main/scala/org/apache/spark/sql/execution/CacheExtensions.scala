package org.apache.spark.sql.execution

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.ZIndexCacheExtensions.ExtensionsBuilder

/**
 * @time 2021/3/4 3:45 下午
 * @author fchen <cloud.chenfu@gmail.com>
 */
class CacheExtensions extends ExtensionsBuilder with Logging {
  override def apply(sessionExtensions: SparkSessionExtensions): Unit = {
    logInfo("register extension CacheExtensions.")
    sessionExtensions.injectResolutionRule(s => ReplaceToDeltaRelation(s))
  }
}

object ZIndexCacheExtensions {
  type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
  type ExtensionsBuilder = SparkSessionExtensions => Unit
}
