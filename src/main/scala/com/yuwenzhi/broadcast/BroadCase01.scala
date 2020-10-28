package com.yuwenzhi.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadCase01 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("WARN:Class Not Find", "INFO:Class Not Find", "DEBUG:Class Not Find"))

    //为啥要采用广播变量：
    // 不采用广播变量的方式，变量list需要发送到每一个Task
    //采用广播变量后： 变量list只需发送到每个Executor,然后每个executor再发送给每个Task
    val list = "WARN"
    val warn: Broadcast[String] = sc.broadcast(list)

    //rdd.filter(_.contains(list)).collect().foreach(println)
    rdd.filter(_.contains(warn.value)).collect().foreach(println)

    //5.关闭
    sc.stop()
  }
}
