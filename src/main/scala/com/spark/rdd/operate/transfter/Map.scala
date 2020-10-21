package com.spark.rdd.operate.transfter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*创建一个1到10数组的RDD，将所有元素*2形成新的RDD*/
object Map {
  def main(args: Array[String]): Unit = {
    //spark环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkRDD")
    val sc = new SparkContext(config)

    //新建RDD
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //转换RDD
    val newRDD: RDD[Int] = listRDD.map((x:Int)=>x*2)

    listRDD.collect().foreach(println)
    newRDD.collect().foreach(println)
  }
}
