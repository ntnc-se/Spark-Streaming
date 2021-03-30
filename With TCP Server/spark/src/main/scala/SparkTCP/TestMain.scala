package SparkTCP

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestMain {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Demo Print Stream Application")
    val ssc = new StreamingContext(conf, Seconds(1))

    val spark = SparkSession
      .builder
      .appName("StructuredNetwork")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 8006)
      .load()

    val data = lines.map(row => {
      val arr = row.getAs[String](0).split("\\|")
      TestModel(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7), arr(8), arr(9), arr(10),
        arr(11), arr(12), arr(13), arr(14), arr(15), arr(16), arr(17), arr(18), arr(19), arr(20), arr(21),
        arr(22), arr(23), arr(24), arr(25), arr(26), arr(27), arr(28), arr(29), arr(30), arr(31), arr(32),
        arr(33), arr(34), arr(35), arr(36), arr(37), arr(38), arr(39))
    })

    //    val windowedCount = data.groupBy(
    //    window($"dateLog", "10 seconds", "5 seconds"), $"state").count()
    //    println("count: "+ windowedCount)

    data.printSchema()

    val query = data.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()


    /**
     * Using TCP Socket with another API SparkKafka.Streaming Context
     * streaming context is main entry point for all streaming function
     * create a local streaming context with two thread and batch 1 second
     *
     */

    //    val data: DStream[String] = ssc.socketTextStream("localhost", 8006)
    //
    //    5 seconds log once
    //    every 5 seconds cut a rdd
    //    var count = 0
    //
    //    data.window(Seconds(5), Seconds(5)).foreachRDD{ rdd =>
    //
    //      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    //
    //      import spark.implicits._
    //
    //      val data = rdd.map( f => f.split("\\|"))
    //
    //      var fileDataFrame = data.toDF("data")
    //
    //      fileDataFrame.show(40)
    //
    //      println("test 1 row: " + fileDataFrame.collect()(10).toString())
    //
    //      if(fileDataFrame.collect()(10).toString().equals("[END]")){
    //        count += 1
    //        println("get")
    //      }else{
    //        println("skip")
    //      }
    //
    //        val values = line.split("\\|")
    //        val state = values(10)
    //        // not order by time
    //        if (state.equals("END")) {
    //          count += 1
    //          println("count: " + count)
    //        }
    //      }
    //
    //    count is not a global variable ~ regex
    //    }
    //
    //    Start the computation
    //    ssc.start()
    //    Wait for the computation to terminate
    //    ssc.awaitTermination()

  }

}
