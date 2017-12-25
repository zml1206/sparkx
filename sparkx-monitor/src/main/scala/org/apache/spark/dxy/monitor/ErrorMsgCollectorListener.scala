package org.apache.spark.dxy.monitor

import org.apache.spark.SparkEnv
import org.apache.spark.dxy.util.MailAgent
import org.apache.spark.scheduler.{JobFailed, SparkListener, SparkListenerApplicationEnd, SparkListenerJobEnd}

/**
 * Created by fchen on 2017/12/14.
 */
class ErrorMsgCollectorListener extends SparkListener {
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    jobEnd.jobResult match {
      case JobFailed(exception) =>
        ErrorMsgCollectorListener.sendMail(jobEnd.jobId.toString, exception.getMessage)
      case _ => // do nothing !
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    applicationEnd

  }
}

object ErrorMsgCollectorListener {
  val enable = SparkEnv.get.conf.getBoolean("spark.error.msg.collect.enable", false)
  val to = SparkEnv.get.conf.get("spark.error.msg.collect.subusers")
  val appName = SparkEnv.get.conf.get("spark.app.name")

  def sendMail(jobId: String, msg: String): Unit = {
    if (enable) {
      val subject = s"Spark App ${appName} [$jobId] failed!!!"
      val agent =
        new MailAgent(to, null, null, "sparkx@smtp.dxy.cn", subject, msg, "10.25.23.11")
      agent.sendMessage
    }
  }
}
