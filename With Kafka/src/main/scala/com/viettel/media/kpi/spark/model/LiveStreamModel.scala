package com.viettel.media.kpi.spark.model

import org.apache.spark.sql.types.{LongType, StringType, StructType}

object LiveStreamModel {
  //Kafka
  val timestamp = "timestamp"

  //Log
  val dateTime = "dateTime" //_c0
  val cdr = "cdr" //_c1
  val channel = "channel" //_c2
  val source = "source" //_c3
  val userID = "userID" //_c4
  val isVip = "isVip" //_c5
  val clientType = "clientType" //_c6
  val reversion = "reversion" //_c6
  val networkType = "networkType" //_c7
  val videoID = "videoID" //_c7
  val state = "state" //_c8
  val timeLog = "timeLog" //_c9
  val lagArr = "lagArr" //_c10
  val playArr = "playArr" //_c11
  val placeHolder0 = "placeHolder0" //_c12
  val averageLag = "averageLag" //_c13
  val averageWatch = "averageWatch" //_14
  val placeHolder1 = "placeHolder1" //_c15
  val price = "price" //_c16
  val ipAddress = "ipAddress" //_c17
  val userAgent = "userAgent" //_c18
  val mediaLink = "mediaLink" //_c19
  val pageLink = "pageLink" //_c20
  val placeHolder2 = "placeHolder2" //_c21
  val errorDesc = "errorDesc" //_c22
  val serverTime = "serverTime" //_c23
  val volumne = "volumne" //_c24
  val domain = "domain" //_c25
  val bandwithArray = "bandwithArray" //_c26
  val networkArray = "networkArray" //_c27
  val isAds = "isAds" //_c28
  val recommandType = "recommandType" //_c29
  val duration = "duration" //_c30
  val isVideolive = "isVideolive" //_c31
  val cateID = "cateID" //_c32
  val gameID = "gameID" //_c33
  val videoType = "videoType" //_c34
  val streamerID = "streamerID" //_c35
  val languageCode = "languageCode" //_c36
  val countryCode = "countryCode" //_c37

  //extracted
  val totalplay = "totalplay"
  val currentView = "currentView"
  val lastViewCache = "lastViewCache"
  val isLag = "isLag"
  val isError = "isError"
  val diffTime = "diffTime"
  //agg
  val total = "total"
  val lagPercentage = "lagPercentage"
  val errorPercentage = "errorPercentage"

  val totalViewer = "totalViewer"
  val totalView = "totalView"
  val totalViewGt5s = "totalViewGt5s"
  val totalViewGt10s = "totalViewGt10s"
  val totalViewGt15s = "totalViewGt15s"
  val totalViewTime = "totalViewTime"
  val highestCurrentView = "highestCurrentView"
  val avgViewTime = "avgViewTime"
  val lastView = "lastView"


  val videoSchema = new StructType()
    .add(dateTime, StringType, false)
    .add(cdr, StringType, false)
    .add(channel, StringType, false)
    .add(source, StringType, false)
    .add(userID, StringType, false)
    .add(isVip, StringType, false)
    .add(clientType, StringType, false)
    .add(reversion, StringType, false)
    .add(networkType, StringType, false)
    .add(videoID, StringType, false)
    .add(state, StringType, false)
    .add(timeLog, StringType, false)
    .add(lagArr, StringType, false)
    .add(playArr, StringType, false)
    .add(placeHolder0, StringType, false)
    .add(averageLag, StringType, false)
    .add(averageWatch, StringType, false)
    .add(placeHolder1, StringType, false)
    .add(price, StringType, false)
    .add(ipAddress, StringType, false)
    .add(userAgent, StringType, false)
    .add(mediaLink, StringType, false)
    .add(pageLink, StringType, false)
    .add(placeHolder2, StringType, false)
    .add(errorDesc, StringType, false)
    .add(serverTime, StringType, false)
    .add(volumne, StringType, false)
    .add(domain, StringType, false)
    .add(bandwithArray, StringType, false)
    .add(networkArray, StringType, false)
    .add(isAds, StringType, false)
    .add(recommandType, StringType, false)
    .add(duration, StringType, false)
    .add(isVideolive, LongType, false)
    .add(cateID, StringType, false)
    .add(gameID, StringType, false)
    .add(videoType, StringType, false)
    .add(streamerID, StringType, false)
    .add(languageCode, StringType, false)
    .add(countryCode, StringType, false)
}
