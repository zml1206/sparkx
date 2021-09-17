package org.apache.spark.sql.sparkcube.catalog

import com.dxy.data.http.{HttpClientUtil, HttpConstants, PKUtil}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.sparkcube.catalog.CubeApiExternalCatalog._
import org.apache.spark.sql.sparkcube.util.JacksonUtil

/**
 * Created by zhuml on 2021/8/9.
 */
class CubeApiExternalCatalog extends CubeExternalCatalog {

  override def getCacheInfos(): Map[String, CacheTableInfo] = {

    val paramsMap = PKUtil.getParams(PK, SparkFiles.get("." + PK), APP_ID, null, null)

    val httpResult = HttpClientUtil.doGet(URL, paramsMap, HttpConstants.DEFAULT)
    if (httpResult.getCode != 200) {
      throw new RuntimeException(s"获取 spark cube 元数据失败：${JacksonUtil.toJson(httpResult)}")
    }
    val dxyHttpBody = JacksonUtil.fromJson(httpResult.getBody, classOf[DxyHttpBody])
    dxyHttpBody.results.map(cti => (cacheKey(cti.sourceType, cti.db, cti.table), cti)).toMap
  }

}

object CubeApiExternalCatalog {
  val PK = "mlsql-skone"
  val URL = "https://odep-api.uc.host.dxy/api/materialized/view"
  val APP_ID = "app_mlsql_init"
}

case class DxyHttpBody(success: Boolean,
                       code: Int,
                       message: String,
                       responseTime: Long,
                       results: Array[CacheTableInfo])
