package com.spark.rdd.operate.transfter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*按分区重组元素*/
object Glom {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setAppName("MapParrtition").setMaster("local[*]")
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 8,3)

    //将一个分区的数据存到一个数组中 得到多个数组
    val glomRDD: RDD[Array[Int]] = listRDD.glom()
    glomRDD.collect().foreach((array: Array[Int]) =>
      println(array.mkString(","))
    )
  }
}
