package com.viettel.media.kpi.service

import com.viettel.media.kpi.App
import com.viettel.media.kpi.dao.LoadHomeDAO
import com.viettel.media.kpi.spark.connector.Datasource
import com.viettel.media.kpi.spark.model.LoadHomeModel
import com.viettel.media.kpi.util.LoggerUtil
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{ForeachWriter, Row}

import java.sql.PreparedStatement

case class LoadHomeService() {
  val loadHomeDao = LoadHomeDAO()
  val loadHomeDf = loadHomeDao.getResponseTime()

  def run(): Unit = {
    loadHomeDf.printSchema()
    val df = loadHomeDf.withColumn(LoadHomeModel.diffTime, concat(col("window.start"), lit("&"), col("window.end")))

    val query = df.writeStream.foreach(new ForeachWriter[Row] {
      var connection: java.sql.Connection = _
      var statement: java.sql.PreparedStatement = _
      override def open(partitionId: Long, version: Long): Boolean = {
        val v_sql = "REPLACE INTO kpi_ttkt.xgaming_home_load_topic(diffTime, responseTime, category) values(?,?,?)"
        connection = Datasource.connectionPool.getConnection
        connection.setAutoCommit(false)
        statement = connection.prepareStatement(v_sql)
        true
      }

      override def process(value: Row): Unit = {
        warningLag(value, 240)
        pushDMOS(value, statement)
      }

      override def close(errorOrNull: Throwable): Unit = {
        println("Close")
        connection.commit()
        connection.close
      }
    }).outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime(300000))
      .start()

    query.awaitTermination()
  }

  def warningLag(value: Row, threshold: Int):Unit = {
    val warningValue = value.getDouble(2)
    if (warningValue > threshold) {
      val warning = value.getStruct(0).getTimestamp(0).toString + " - " +
        value.getStruct(0).getTimestamp(1).toString +
        " Load Response Time Category " + value.getString(1) + " : " + value.getDouble(2)
      //    LoggerUtil.sendLog(warning)
      println("warning|" + warning)
      LoggerUtil.sendLogGroup("0969664623_1616462911442", warning)
    }
  }

  def pushDMOS(value: Row, statement: PreparedStatement): Unit ={
    statement.setString(1, value(0).toString)
    statement.setDouble(2, value(2).toString.toDouble)
    statement.setString(3, value(1).toString)
    statement.executeUpdate()
  }
}
