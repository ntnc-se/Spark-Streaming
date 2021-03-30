package com.viettel.media.kpi.spark

import com.viettel.media.kpi.App
import com.viettel.media.kpi.conf.Configure
import com.viettel.media.kpi.spark.connector.MySqlConnector
import com.viettel.media.kpi.spark.model.cms.MysqlModel
import com.viettel.media.kpi.util.LoggerUtil
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

trait Processor {
  val spark: SparkSession = Option(App.spark) match {
    case None => sys.error("spark session is not loaded")
    case Some(spark) => spark
  }

  val dMOSConnector = MySqlConnector(App.configFile.MySqlHost, App.configFile.MySqlUser, App.configFile.MySqlPass)

  def saveCMS(seq : Row, model: MysqlModel, sc: SparkSession): Unit = {
    val spark_ = sc
    val rowList = new java.util.ArrayList[Row]()
    rowList.add(seq)
    val df = spark_.createDataFrame(rowList, model.getSchema)
    dMOSConnector.write(df, model.getTable)
  }

  def subKafka(kafkaHost: String, topic: String): DataFrame = {
    val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaHost)
        .option("subscribe", topic)
        .load()
//    df.printSchema()
    df
  }
}
