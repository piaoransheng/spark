package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming13_Continue {
  def main(args: Array[String]): Unit = {

    //第一个参数是指定位置，第二个参数是获取StreamingContext
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", () => {
      getStreamingContext()
    })


    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取StreamingContext
    *
    * @return StreamingContext
    */
  def getStreamingContext(): StreamingContext = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    ds.print()
    ssc
  }

}
