package com.viettel.media.kpi.spark.connector

import com.viettel.media.kpi.App
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

case class MySqlConnector(connectStr: String, user: String, pass: String) extends Connector {
  Class.forName("com.mysql.jdbc.Driver")
  val spark = App.spark

  override def write(df: DataFrame, tableName: String): Unit = {
    val properties = new Properties()
    properties.put("user",user)
    properties.put("password",pass)

    df.write.mode(SaveMode.Append).option("encoding", "UTF-8").option("characterEncoding", "UTF-8")
      .jdbc(connectStr, tableName, properties)
  }

  override def read(tableName: String): DataFrame = {
    val properties = new Properties()
    properties.put("user",user)
    properties.put("password",pass)

    val df = spark.read.jdbc(connectStr, tableName, properties)
    df
  }

}
