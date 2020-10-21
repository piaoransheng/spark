package com.spark.rdd.operate.transfter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*扁平化  将一个集合拆成多个集合*/
object FlatMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)

    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1,2),List(3,4)))
    val flatMapRDD: RDD[Int] = listRDD.flatMap((datas: List[Int]) =>datas)
    listRDD.collect().foreach(println)
    flatMapRDD.collect().foreach(println)
  }
}
