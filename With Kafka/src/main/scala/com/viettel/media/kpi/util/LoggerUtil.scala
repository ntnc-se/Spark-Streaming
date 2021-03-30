package com.viettel.media.kpi.util

import com.viettel.media.kpi.App
import com.viettel.media.kpi.conf.Configure

import java.io.IOException
import scalaj.http.Http

import java.net.URLEncoder

object LoggerUtil {
  val urlGroup = "http://10.60.139.240:7000/privateapi/keeng/sendMucMessage"
  val urlBase="http://10.60.139.240:7000/privateapi/usefortest/send?content=:content&from=MOCHA&to=:phone&u=vt13579"

  def sendLog(log: String): Unit = {
    val logline = "%s-%s".format(System.currentTimeMillis(), log)
    val url = urlBase
      .replace(":content", URLEncoder.encode(logline, "UTF-8"))
      .replace(":phone", "0969664623")
    Http(url).asString
  }

  def sendLogGroup(groupId: String, log: String, retries:Int = 3): Unit = {
    val json = Seq("username" -> "report",
      "pwd" -> "report@#$2019",
      "from_msisdn" -> "REPORT",
      "group_id" -> groupId,
      "content" -> log)
    try {
      if(retries == 0) {
        throw new Exception("send log group noti failed")
      }
      try {
        Http(urlGroup).timeout(connTimeoutMs = 1000, readTimeoutMs = 5000).postForm(json).asString
      } catch {
        case _: Throwable => throw new IOException("error catch when call api")
      }
    } catch {
      case _: IOException => sendLogGroup(groupId, log,retries - 1)
    }
  }
}
