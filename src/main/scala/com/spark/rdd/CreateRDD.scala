package com.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//创建RDD
object CreateRDD {
  def main(args: Array[String]): Unit = {
    //spark环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkRDD")
    val sc = new SparkContext(config)

    //1.从内存创建RDD  makeRDD和parallelize
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val listRDD2: RDD[Int] = sc.parallelize(Array(1,2,3))


    //2。从外部存储中创建:默认情况下可以读取项目路径，也可以读取其他路径包括hdfs
//    val RDD1: RDD[String] = sc.textFile("in",2)   //后面的2表示分区
//    val RDD2: RDD[String] = sc.textFile("hdfs://hadoop102:9000/RELEASE")

    listRDD.collect().foreach(println)
    println(listRDD2.collect().mkString(","))

    //将RDD的数据保存到文件
//    listRDD.saveAsTextFile("output")
  }
}
