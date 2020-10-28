package com.yuwenzhi.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test


class Test_wordCount {
  @Test
  //方法一
  def test_scalaWC(): Unit = {
    var lineData = List(
      "hello java",
      "hello scala",
      "hello world",
      "hello spark",
      "hello flink",
      "hello spark flink form scala",
    )
//    lineData.flatMap(_.split(" ")).groupBy(word=>word).map{
//      case (word,list)=>{
//        (word,list.size)
//      }
//    }.foreach(print)
    lineData.flatMap(_.split(" ")).groupBy(w=>w).map(kv=>(kv._1,kv._2.length)).foreach(print)
  }

  @Test
  //方法二
  def test_TestSparkWC(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val result: RDD[(String, Int)] = sc.textFile("input1").flatMap(_.split("\\s+")).map(w=>(w,1)).reduceByKey(_+_)
    result.collect().foreach(println)

    //5.关闭
    sc.stop()
  }

  @Test
  //方法三
  def test_TestSparkWC2(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val groupRDD: RDD[(String, Iterable[String])] = sc.textFile("input1").flatMap(_.split("\\s+")).groupBy(w=>w)
    groupRDD.map{
      case kv =>{
        (kv._1,kv._2.size)
      }
    }.foreach(println)

    //5.关闭
    sc.stop()
  }
}
