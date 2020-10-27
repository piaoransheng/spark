package com.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_Kafka {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //TODO Kafka配置信息
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "ip1:port1,ip2:port2,ip3:port3",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguitu",
      "key.deserializer" -> "org.apache.kafka.common.serialization,StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //TODO 使用SparkStreaming读取kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("atguigu"),
        kafkaPara)
    )
    //TODO 消费
    val valueDStream: DStream[String] = kafkaDStream.map((record: ConsumerRecord[String, String]) => record.value())
    valueDStream.flatMap((_: String).split(" "))
      .map((_: String, 1))
      .reduceByKey((_: Int) + (_: Int))
      .print()


    ssc.start()
    ssc.awaitTermination()
  }
}
