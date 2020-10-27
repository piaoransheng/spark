package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming02_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))


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

    ssc.start()
    ssc.awaitTermination()
  }
}
