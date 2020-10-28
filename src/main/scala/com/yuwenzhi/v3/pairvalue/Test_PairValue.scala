package com.yuwenzhi.v3.pairvalue

import com.yuwenzhi.v3.key_vlaue.Test_Value
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Test}

class Test_PairValue {
  @After
  def close(): Unit ={
    Test_Value.sc.stop()
  }

  @Test
  def Test_intersection(): Unit ={
    val rdd1: RDD[Int] = Test_Value.sc.makeRDD(1 to 4,1)
    val rdd2: RDD[Int] = Test_Value.sc.makeRDD(4 to 7,1)
    rdd1.intersection(rdd2).collect().foreach(println)
  }

  @Test
  def Test_union(): Unit ={
    val rdd1: RDD[Int] = Test_Value.sc.makeRDD(1 to 4,1)
    val rdd2: RDD[Int] = Test_Value.sc.makeRDD(1 to 4,1)
    rdd1.union(rdd2).collect().foreach(println)
  }

  @Test
  def Test_subtract(): Unit ={
    //差集
    val rdd1: RDD[Int] = Test_Value.sc.makeRDD(1 to 4,1)
    val rdd2: RDD[Int] = Test_Value.sc.makeRDD(4 to 5,1)
    rdd1.subtract(rdd2).collect().foreach(println)
  }

  @Test
  def Test_zip(): Unit ={
    val rdd1: RDD[Int] = Test_Value.sc.makeRDD(1 to 4,1)
    val rdd2: RDD[Int] = Test_Value.sc.makeRDD(4 to 5,1)
    rdd1.zip(rdd2).collect().foreach(println)
  }
}

object Test_PairValue{
  //2.创建SparkContext，该对象是提交Spark App的入口
  val sc: SparkContext = new SparkContext(new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]"))
}
