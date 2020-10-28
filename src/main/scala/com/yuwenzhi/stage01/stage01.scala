package com.yuwenzhi.stage01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object stage01 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val resultRdd: RDD[(Int, Int)] = rdd.map((_,1)).reduceByKey(_+_)

    resultRdd.collect().foreach(println)

    resultRdd.saveAsTextFile("output2")

    Thread.sleep(10000000)
    //5.关闭
    sc.stop()
  }
}
