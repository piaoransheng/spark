package com.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//创建RDD
object TestRDD {
  def main(args: Array[String]): Unit = {
    //spark环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkRDD")
    val sc = new SparkContext(config)

    //1.从内存创建RDD  makeRDD和parallelize
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val listRDD2: RDD[Int] = sc.parallelize(Array(1,2,3))


    listRDD2.collect().foreach(println)
    println(listRDD.collect().mkString(","))

  }
}
