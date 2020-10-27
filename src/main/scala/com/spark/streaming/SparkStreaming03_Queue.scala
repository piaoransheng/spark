package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


/**
  * 模拟一个队列当做数据源  代替netCat或者Kafka
  */
object SparkStreaming03_Queue {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //    val rdd: RDD[Int] = ssc.sparkContext.makeRDD(List(1, 2, 3, 4))
    //    rdd.collect().foreach(println)

    //声明一个队列
    val queue = new mutable.Queue[RDD[String]]()
    //队列数据
    val queueDS: InputDStream[String] = ssc.queueStream(queue)
    //打印队列数据
    queueDS.print()

    ssc.start()
    //往队列加数据
    for (i <- 1 to 5) {
      val rdd: RDD[String] = ssc.sparkContext.makeRDD(List(i.toString))
      queue.enqueue(rdd)
      Thread.sleep(1000)
    }
    ssc.awaitTermination()
  }
}
