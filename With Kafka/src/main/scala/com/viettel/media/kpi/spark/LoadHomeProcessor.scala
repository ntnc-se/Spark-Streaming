package com.viettel.media.kpi.spark

import com.viettel.media.kpi.spark.model.LoadHomeModel
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, functions}

case class LoadHomeProcessor(kafkaHost: String, topic: String) extends Processor {
  val liveDf: DataFrame = loadLoadHomeData(kafkaHost, topic)

  def loadLoadHomeData(kafkaHost: String, topic: String): DataFrame = {
    println("kafka|" + kafkaHost + "|" + topic)
    val df = this.subKafka(kafkaHost, topic)
      .select(col(LoadHomeModel.timestamp),functions.split(col("value").cast("string"),"\\|").as("value"))
      .select(col(LoadHomeModel.timestamp),
          col("value").getItem(0).as(LoadHomeModel.dateTime),
        col("value").getItem(1).as(LoadHomeModel.category),
        col("value").getItem(2).as(LoadHomeModel.responseTime).cast(IntegerType))
    df
  }
}
