package com.spark.rdd.operate.transfter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setAppName("MapParrtition").setMaster("local[*]")
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    val indexRDD1: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) =>                    //num分区号，datas就是listRDD
        datas.map((x:Int) => (x, "分区号" + num))    //data表示listRDD里面的每个元素
    }

    val indexRDD2: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) =>                     //num分区号，datas就是listRDD
        datas.map((_:Int, "分区号：" + num))        //下横杠表示listRDD里面的每个元素
    }


    indexRDD1.collect().foreach(println)
    indexRDD2.collect().foreach(println)
  }
}
