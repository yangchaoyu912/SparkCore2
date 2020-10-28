package com.yuwenzhi.v1.pair_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class Test_pair_value_operator {

  //intersection(): 求两个Rdd的交集，返回新的Rdd
  @Test
  def Test_intersection(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 4,1)
    val rdd2: RDD[Int] = sc.makeRDD(3 to 9,1)

    rdd1.intersection(rdd2).collect().foreach(println)

    //5.关闭
    sc.stop()
  }

  //union(): 并集
  @Test
  def Test_union(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 4,1)
    val rdd2: RDD[Int] = sc.makeRDD(3 to 9,1)

    rdd1.union(rdd2).collect().foreach(println)

    //5.关闭
    sc.stop()
  }

  //subtract() : 差集，去除rdd中两个rdd相同的元素
  @Test
  def Test_subtract(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 4,1)
    val rdd2: RDD[Int] = sc.makeRDD(3 to 9,1)

    rdd1.subtract(rdd2).collect().foreach(println)

    //5.关闭
    sc.stop()
  }

  @Test
  //zip: 拉链，即两个rdd中value， 以键值对的形式进行合并
  def Test_zip(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 4,4)
    val rdd2: RDD[Int] = sc.makeRDD(3 to 6,4)

    rdd1.zip(rdd2).collect().foreach(println)

    //5.关闭
    sc.stop()
  }



}
