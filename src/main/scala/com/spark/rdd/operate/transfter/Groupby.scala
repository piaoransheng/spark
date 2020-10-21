package com.spark.rdd.operate.transfter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//分组
object Groupby {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    val groupRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy((i: Int) =>i%2)
    groupRDD.collect().foreach(println)

    val filterRDD: RDD[Int] = listRDD.filter((f:Int)=>f.equals(5))
    filterRDD.collect().foreach(println)
  }
}
