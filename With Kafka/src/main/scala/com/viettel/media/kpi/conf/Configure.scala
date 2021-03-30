package com.viettel.media.kpi.conf

import com.viettel.media.kpi.App

import java.io.FileInputStream
import java.util.Properties

case class Configure(configPath: String)   {
  val properties: Properties = new Properties()
  Option(new FileInputStream(configPath)) match {
    case None => sys.error("Configure file not found")
    case Some(resource) => {
      properties.load(resource)
    }
  }

  val KafkaHost: String = properties.getProperty("kafka.host")
  val XgamingLiveStreamTopic: String = properties.getProperty("xgaming.live.stream.topic")
  val XgamingLoadHomeTopic: String = properties.getProperty("xgaming.load.home.topic")

  val UrlGroupConfig: String = properties.getProperty("config.url.group")
  val GroupIdConfig: String = properties.getProperty("config.group.id")
  val LagThreshold: Int = properties.getProperty("config.lag.threshold").toInt
  val LoadHomeThreshold: Int = properties.getProperty("config.load.home.threshold").toInt

  val MySqlHost: String = properties.getProperty("config.mysql.host")
  val MySqlUser: String = properties.getProperty("config.mysql.user")
  val MySqlPass: String = properties.getProperty("config.mysql.pass")
}
