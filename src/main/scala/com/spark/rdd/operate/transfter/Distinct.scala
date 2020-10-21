package com.spark.rdd.operate.transfter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Distinct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,1,1,2,3,2,3,5,6))
    val distinctRDD: RDD[Int] = listRDD.distinct()
    distinctRDD.collect().foreach(println)

    val distinctRDD2: RDD[Int] = listRDD.distinct(1)
    distinctRDD2.collect().foreach(println)
    distinctRDD.saveAsTextFile("output")
  }
}
