package com.viettel.media.kpi.dao

import com.viettel.media.kpi.App
import com.viettel.media.kpi.spark.LoadHomeProcessor
import com.viettel.media.kpi.spark.model.LoadHomeModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, count, desc, when, window}

case class LoadHomeDAO() {
  val loadHomeProcessor: LoadHomeProcessor = LoadHomeProcessor(App.configFile.KafkaHost,
    App.configFile.XgamingLoadHomeTopic)
  val df = loadHomeProcessor.liveDf

  def getResponseTime(): DataFrame = {
//    val records = df.count()
//    val getNumbers = (records*0.1).toInt

    val loadHomeDf = df.groupBy(window(col(LoadHomeModel.timestamp),"5 minutes", "5 minutes"),
      col(LoadHomeModel.category))
      .agg(avg(LoadHomeModel.responseTime).as("avg"))
    println("load home schema|")
    loadHomeDf.printSchema()
    loadHomeDf
  }

}
