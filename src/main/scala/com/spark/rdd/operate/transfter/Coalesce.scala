package com.spark.rdd.operate.transfter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Coalesce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16,4)
    println(listRDD.partitions.size)

    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)
    println(coalesceRDD.partitions.size)
  }
}
