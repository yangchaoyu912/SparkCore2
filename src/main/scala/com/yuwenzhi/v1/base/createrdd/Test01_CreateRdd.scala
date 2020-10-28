package com.yuwenzhi.v1.base.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class Test01_CreateRdd {
  // 1. 从集合中创建
  @Test
  def test01: Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3. 使用parallelize()创建rdd
    val rdd: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6))

    //4. 使用makeRDD()创建rdd
    val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,4,5))

    rdd1.collect.foreach(println)

    rdd.collect.foreach(println)

    //5.关闭
    sc.stop()
  }

  // 2. 从外部存储系统中数据集中创建rdd
  @Test
  def test02(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    sc.textFile("hdfs://hadoop102:8020/input").foreach(println)

    //5.关闭
    sc.stop()
  }

}
