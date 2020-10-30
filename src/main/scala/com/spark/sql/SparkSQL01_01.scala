package com.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL01_01 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val ss: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //1.读取文件获取dataFrame
    val jsonDF: DataFrame = ss.read.json("input/user.json")
    //2.将dataFrame转换为临时视图
    jsonDF.createOrReplaceTempView("user")

    //TODO sql查询
    ss.sql("select * from user").show()

    //TODO jsonDF查询
    jsonDF.select("name", "age").show()
  }

}
