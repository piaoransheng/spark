package com.spark.rdd.operate.transfter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitions {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setAppName("MapParrtition").setMaster("local[*]")
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions((datas: Iterator[Int]) =>{
      datas.map((data:Int)=>data*2)
    })
    mapPartitionsRDD.collect().foreach(println)
  }
}
