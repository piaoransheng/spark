package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {

    //TODO 环境变量
    //1.创建SparkConfig对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    //2.上下文对象  3000ms等于3s等于数据采集周期，每3s采集一次数据
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //    val context= new StreamingContext(config,Milliseconds(3000))
    //    val context= new StreamingContext(config,Seconds(3000))
    //    val context= new StreamingContext(config,Minutes(3000))


    //TODO 执行逻辑
    //1.从socket获取数据，数据是一行一行获取的
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    //2.行扁平化得到每个单词
    val wordDS: DStream[String] = socketDS.flatMap((_: String).split(" "))
    //3.每个单词转换为(word,1)格式
    val wordToOneDS: DStream[(String, Int)] = wordDS.map((_: String, 1))
    //4.技术每个单词的个数(word,n)
    val wordToSumDS: DStream[(String, Int)] = wordToOneDS.reduceByKey((_: Int) + (_: Int))
    //5.打印
    wordToSumDS.print()

    //TODO 关闭环境 实时不用stop
    //启动采集器
    ssc.start()
    //等待采集器的结束
    ssc.awaitTermination()
  }
}