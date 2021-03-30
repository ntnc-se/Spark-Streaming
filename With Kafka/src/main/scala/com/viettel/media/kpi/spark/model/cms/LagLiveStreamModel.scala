package com.viettel.media.kpi.spark.model.cms

import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructType}

object LagLiveStreamModel extends MysqlModel {
  val diffTime = "diffTime"
  val lagPercentage = "lagPercentage"

  val tableName = "test_lag_livestream"

  val lagLiveStreamSchema: StructType = new StructType()
    .add(diffTime, StringType, false)
    .add(lagPercentage, DoubleType, false)

  override def getTable: String = tableName

  override def getSchema: StructType = lagLiveStreamSchema
}
