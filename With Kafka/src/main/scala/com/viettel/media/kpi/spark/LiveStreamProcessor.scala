package com.viettel.media.kpi.spark

import com.viettel.media.kpi.spark.model.LiveStreamModel
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, functions}

case class LiveStreamProcessor(kafkaHost: String, topic: String) extends Processor {
  val liveDf: DataFrame = loadLiveStreamData(kafkaHost, topic)

  def loadLiveStreamData(kafkaHost: String, topic: String): DataFrame = {
    val df = this.subKafka(kafkaHost, topic)
      .select(col(LiveStreamModel.timestamp),functions.split(col("value").cast("string"),"\\|").as("value"))
      .select(col(LiveStreamModel.timestamp),
          col("value").getItem(0).as(LiveStreamModel.dateTime),
        col("value").getItem(1).as(LiveStreamModel.cdr),
        col("value").getItem(2).as(LiveStreamModel.channel),
        col("value").getItem(3).as(LiveStreamModel.source),
        col("value").getItem(4).as(LiveStreamModel.userID),
        col("value").getItem(5).as(LiveStreamModel.isVip),
        col("value").getItem(6).as(LiveStreamModel.clientType),
        col("value").getItem(7).as(LiveStreamModel.reversion),
        col("value").getItem(8).as(LiveStreamModel.networkType),
        col("value").getItem(9).as(LiveStreamModel.videoID),
        col("value").getItem(10).as(LiveStreamModel.state),
        col("value").getItem(11).as(LiveStreamModel.timeLog),
        col("value").getItem(12).as(LiveStreamModel.lagArr),
        col("value").getItem(13).as(LiveStreamModel.playArr),
        col("value").getItem(14).as(LiveStreamModel.placeHolder0),
        col("value").getItem(15).as(LiveStreamModel.averageLag),
        col("value").getItem(16).as(LiveStreamModel.averageWatch),
        col("value").getItem(17).as(LiveStreamModel.placeHolder1),
        col("value").getItem(18).as(LiveStreamModel.price),
        col("value").getItem(19).as(LiveStreamModel.ipAddress),
        col("value").getItem(20).as(LiveStreamModel.userAgent),
        col("value").getItem(21).as(LiveStreamModel.mediaLink),
        col("value").getItem(22).as(LiveStreamModel.pageLink),
        col("value").getItem(23).as(LiveStreamModel.placeHolder2),
        col("value").getItem(24).as(LiveStreamModel.errorDesc),
        col("value").getItem(25).as(LiveStreamModel.serverTime),
        col("value").getItem(26).as(LiveStreamModel.volumne),
        col("value").getItem(27).as(LiveStreamModel.domain),
        col("value").getItem(28).as(LiveStreamModel.bandwithArray),
        col("value").getItem(29).as(LiveStreamModel.networkArray),
        col("value").getItem(30).as(LiveStreamModel.isAds),
        col("value").getItem(31).as(LiveStreamModel.recommandType),
        col("value").getItem(32).as(LiveStreamModel.duration),
        col("value").getItem(33).as(LiveStreamModel.isVideolive),
        col("value").getItem(34).as(LiveStreamModel.cateID),
        col("value").getItem(35).as(LiveStreamModel.gameID),
        col("value").getItem(36).as(LiveStreamModel.videoType),
        col("value").getItem(37).as(LiveStreamModel.streamerID),
        col("value").getItem(38).as(LiveStreamModel.languageCode),
        col("value").getItem(39).as(LiveStreamModel.countryCode))
    df
  }
}
