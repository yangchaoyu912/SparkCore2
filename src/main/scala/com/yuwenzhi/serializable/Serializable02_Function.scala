package com.yuwenzhi.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Serializable02_Function {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world","hello scala","hello spark"))

    val search = new Search("hello")

    search.getMatch1(rdd).collect().foreach(println)
    //5.关闭
    sc.stop()
  }
}

class Search(query:String) extends Serializable{
  def isMatch(s:String):Boolean={
    s.contains(query)
  }
  //函数序列化案例
  def getMatch1(rdd:RDD[String])={
    rdd.filter(isMatch)
  }
}
