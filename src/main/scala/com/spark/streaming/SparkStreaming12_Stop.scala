package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//DStreaming转成RDD
object SparkStreaming12_Stop {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))


    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val newDS: DStream[String] = ds.transform((rdd: RDD[String]) =>{
      rdd.map((_: String)*2)
    })
    newDS.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
