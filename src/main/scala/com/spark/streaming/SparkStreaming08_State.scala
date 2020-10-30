package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//数据转成有状态
object SparkStreaming08_State {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //设置检查点位置 会在项目中创建一个cp的文件夹存放缓冲数据
    ssc.sparkContext.setCheckpointDir("cp")

    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordList: DStream[String] = ds.flatMap((_: String).split(" "))
    val wordToOne: DStream[(String, Long)] = wordList.map((_: String, 1))
    //updateStateByKey：有状态计算方法，第一个参数表示相同key的value的集合，第二个参数表示缓冲区用于保存结果数据
    wordToOne.updateStateByKey(
      (valueList: Seq[Long], buffer:Option[Long])=>{
        val newBuffer: Long = buffer.getOrElse(0L) + valueList.sum
        Option(newBuffer)
      }
    ).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
