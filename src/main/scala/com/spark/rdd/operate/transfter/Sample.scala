package com.spark.rdd.operate.transfter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*随机抽样*/
object Sample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16)
    val sampleRDD: RDD[Int] = listRDD.sample(withReplacement = false,0.6,1)
    sampleRDD.collect().foreach(println)
  }
}
