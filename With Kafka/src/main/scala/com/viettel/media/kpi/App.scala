package com.viettel.media.kpi

import com.viettel.media.kpi.conf.Configure
import com.viettel.media.kpi.service.{LiveStreamService, LoadHomeService}
import com.viettel.media.kpi.util.LoggerUtil
import org.apache.spark.sql.SparkSession

import java.io.{PrintWriter, StringWriter}

object App {
  var configFile: Configure = _
  var spark: SparkSession = _

  def setSparkSession(spark: SparkSession): Unit = {
    this.spark = spark
  }

  def setConfigFile(args: Array[String]): Unit = {
    this.configFile = Configure("/u02/vbi_app/ETL/ETL_Repo/vtm_bi_datalake/kpi_ttkt/config.properties")
  }


  def main(args: Array[String]): Unit = {
    try {
//      System.setProperty("hadoop.home.dir", "E:\\winutil\\bin\\winutils.exe")
      val spark = SparkSession
        .builder()
        .appName("ResponseTime-KPI")
        .master("yarn")
        .getOrCreate()

      setConfigFile(args)
      setSparkSession(spark)


//      val liveStreamService = LiveStreamService()
//      liveStreamService.run()
      val loadHomeService = LoadHomeService()
      loadHomeService.run()
    } catch {
      case e: Exception => {
        val errors = new StringWriter()
        e.printStackTrace(new PrintWriter(errors))
        LoggerUtil.sendLogGroup(configFile.GroupIdConfig,errors.toString)
      }
    }
  }
}
