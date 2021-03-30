package SparkTCP

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import org.apache.spark.streaming.{Seconds, StreamingContext}

object App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("demo")
      .master("local[*]")
      .getOrCreate()

    val streaming = new StreamingContext(spark.sparkContext, Seconds(5))

    val server = "localhost:9092"

    val params = Map[String, Object](
      "bootstrap.servers" -> server,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "group.id" -> "dashboard",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topic = "test-demo-ntnc"

    val stream = KafkaUtils.createDirectStream[String, String](
      streaming, PreferBrokers, Subscribe[String, String](topic, params))

    /**
     * for insert db
     */

    val schema = StructType(
      StructField("time", TimestampType) ::
      StructField("category", StringType) ::
      StructField("time-response", FloatType) :: Nil
    )

    type Record =ConsumerRecord[String, String]

    stream.foreachRDD { rdd =>
      rdd.foreachPartition{line =>
        println("Streaming line: " + line)
      }
    }

    // create streaming context and submit streaming jobs
    streaming.start()

    // wait to killing signals etc.
    streaming.awaitTermination()

  }

}
