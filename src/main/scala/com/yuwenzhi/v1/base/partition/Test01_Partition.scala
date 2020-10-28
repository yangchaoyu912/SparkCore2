package com.yuwenzhi.v1.base.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class Test01_Partition {

  // 默认的分区 = 总的cups核数
  @Test
  def Test_default_partition(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,3,4))

    rdd1.saveAsTextFile("output")

    //5.关闭
    sc.stop()
  }

  @Test
  def Test_partition_Array(): Unit ={
    //1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //4个数据，分4个区
    val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,3,4),4)  // 0->1    1->2    2->3    3->4

    val rdd2: RDD[Int] = sc.makeRDD(Array(1,2,3,4),3)  // 0->1    1->2    2->3, 4

    val rdd3: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5),3)  //0->1   1->2, 3   2->3, 4

    rdd1.saveAsTextFile("output1")
    rdd2.saveAsTextFile("output2")
    rdd3.saveAsTextFile("output3")

    //5.关闭
    sc.stop()
  }

  @Test
  def Test_file_default(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    sc.textFile("input/2.txt",3).saveAsTextFile("output2")

    //5.关闭
    sc.stop()
  }
}
