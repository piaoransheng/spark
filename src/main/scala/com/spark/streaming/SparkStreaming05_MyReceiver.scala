package com.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

//TODO 自定义数据采集器
//1.继承抽象类Receiver,Receiver的构造函数是有参的，所以需要传参数
//2.重写方法onStart和onStop方法
class SparkStreaming05_MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  private var socket: Socket = _

  //定义一个方法接收数据
  def receiver(): Unit = {
    var str: String = null
    val bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream,"UTF-8"))
    while (true) {
      str = bufferedReader.readLine()
      if (str != null) {
        //将获取到的数据保存到框架内部进行封装
        store(str)
      }
    }
  }

  //重写启动方法
  override def onStart(): Unit = {
    socket = new Socket(host, port)
    new Thread("Socket Receiver") {
      setDaemon(true)

      override def run(): Unit = {
        receiver()
      }
    }.start()
  }

  //重写结束方法
  override def onStop(): Unit = {
    socket.close()
    socket = null
  }
}
