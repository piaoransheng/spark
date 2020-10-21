package com.spark.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

//案例1：整数累加
object Acc1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
    val sc: SparkContext = new SparkContext(sparkConf)
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    //1.声明累加器变量
    val acc: LongAccumulator = sc.longAccumulator("acc")
    //2.使用累加器
    listRDD.foreach((x: Int) =>
      acc.add(x)
    )
    //3.获取累加器的结果
    println(acc.value)
  }
}
