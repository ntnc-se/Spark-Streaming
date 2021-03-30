package com.viettel.media.kpi.spark.model

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

object LoadHomeModel {
  //Kafka
  val timestamp = "timestamp"

  //Log
  val dateTime = "dateTime" //_c0
  val category = "category" //_c1
  val responseTime = "responseTime" //_c2

  val diffTime = "diffTime"


  val loadHomeSchema = new StructType()
    .add(dateTime, StringType, false)
    .add(category, StringType, false)
    .add(responseTime, IntegerType, false)
}
