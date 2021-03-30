package com.viettel.media.kpi.spark.connector

import org.apache.spark.sql.DataFrame

trait Connector {
  def write(df: DataFrame, tableName: String): Unit
  def read(tableName: String) : DataFrame
}
