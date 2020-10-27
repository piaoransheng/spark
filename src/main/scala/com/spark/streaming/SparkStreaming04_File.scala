package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_File {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val dirDS: DStream[String] = ssc.textFileStream("input")
    dirDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
