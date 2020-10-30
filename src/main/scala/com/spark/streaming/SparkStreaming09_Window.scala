package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//DStreaming转成RDD
object SparkStreaming09_Window {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))


    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val wordList: DStream[String] = ds.flatMap((_: String).split(" "))
    val wordToOne: DStream[(String, Int)] = wordList.map((_: String,1))
    val window: DStream[(String, Int)] = wordToOne.window(Seconds(9))
    //Seconds(9)表示窗口的范围大小，9s就是三个采集周期
    //Seconds(6)表示滑动幅度，没有写的话就是默认采集周期为3s，写的话就是每次滑动6s
    //上面两个：窗口的范围大小和滑动幅度都应该为采集周期的整数倍
//    val window: DStream[(String, Int)] = wordToOne.window(Seconds(9),Seconds(6))
    val resultDS: DStream[(String, Int)] = window.reduceByKey((_: Int)+(_: Int) )
    resultDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
