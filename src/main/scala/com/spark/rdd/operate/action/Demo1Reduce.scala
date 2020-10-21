package com.spark.rdd.operate.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo1Reduce {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("reduceOpereator")
    val sc: SparkContext = new SparkContext(sparkConf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val listRDD2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a",1),
      ("b",2),
      ("a",3),
    ))






    sc.stop()
  }
}
