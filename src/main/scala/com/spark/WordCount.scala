package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
; object WordCount{
  def main(args: Array[String]): Unit = {
    //1.创建SparkConfig对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //2.创建Spark上下文对象
    val sc = new SparkContext(config)

    //3.读取文件，将文件内容一行一行读取出来
    val lines: RDD[String] = sc.textFile("input/word.txt")
    //4.扁平化，  将一行一行的数据分解成一个一个的单词
    val words: RDD[String] = lines.flatMap((_: String).split("  "))
    //5.为了统计方便，将单词数据进行结构转换
    val wordToOne: RDD[(String, Int)] = words.map((_: String, 1))
    //6.对转换后的结构数据进行分组聚合
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey((_: Int)+(_: Int))
    //6.采集结果数据
    val result: Array[(String, Int)] = wordToSum.collect()

    //7.将统计结果大隐刀控制台
    result.foreach(println)
  }
}