package com.viettel.media.kpi.dao

import com.viettel.media.kpi.App
import com.viettel.media.kpi.conf.Configure
import com.viettel.media.kpi.spark.LiveStreamProcessor
import com.viettel.media.kpi.spark.model.LiveStreamModel
import com.viettel.media.kpi.spark.model.cms.LagLiveStreamModel
import com.viettel.media.kpi.util.LoggerUtil
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, when, window}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

case class LiveStreamDAO() {
  val liveStreamProcessor: LiveStreamProcessor = LiveStreamProcessor(App.configFile.KafkaHost,
    App.configFile.XgamingLiveStreamTopic)
  val df = liveStreamProcessor.liveDf

  def getLag(): DataFrame = {
    df.withColumn(LiveStreamModel.isLag,
      when(col(LiveStreamModel.state) === "END"
        && col(LiveStreamModel.playArr).like("%:0,%")
        && !col(LiveStreamModel.playArr).like("%:1%"), 1).otherwise(0))
      .groupBy(window(col(LiveStreamModel.timestamp),"5 minutes", "5 minutes"))
      .agg(((count(when(col(LiveStreamModel.isLag) === 1, true))/count(col(LiveStreamModel.videoID)))*100)
        .as(LiveStreamModel.lagPercentage))
  }

  def getError(): DataFrame = {
    df.withColumn(LiveStreamModel.isError,
      when(col(LiveStreamModel.state) === "END", 1).otherwise(0))
      .groupBy(window(col(LiveStreamModel.timestamp),"5 minutes", "5 minutes"))
      .agg(((count(when(col(LiveStreamModel.isError) === 1, true))/count(col(LiveStreamModel.videoID)))*100)
        .as(LiveStreamModel.errorPercentage))
  }

  def pushMySql(row: Row, spark: SparkSession): Unit = {
    val newRow = RowFactory.create(
      row.getAs[String](LiveStreamModel.diffTime),
      Double.box(row.getAs[Double](LiveStreamModel.lagPercentage))
    )
    liveStreamProcessor.saveCMS(newRow, LagLiveStreamModel, spark)
  }
}
