package com.viettel.media.kpi.spark.connector

import org.apache.commons.dbcp2._

object Datasource {
//  val dbUrl = "jdbc:mysql://127.0.0.1:3306/kpi_ttkt"
  val dbUrl = "jdbc:mysql://10.60.139.183:3306/kpi_ttkt"
  val connectionPool = new BasicDataSource()

  connectionPool.setUsername("admin")
  connectionPool.setPassword("mEdia@#2021")
  connectionPool.setDriverClassName("com.mysql.jdbc.Driver")
  connectionPool.setUrl(dbUrl)
  connectionPool.setMaxTotal(3)
}