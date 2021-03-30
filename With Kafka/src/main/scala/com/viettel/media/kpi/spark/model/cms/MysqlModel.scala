package com.viettel.media.kpi.spark.model.cms

import org.apache.spark.sql.types.StructType

trait MysqlModel {
  def getTable : String
  def getSchema : StructType
}
