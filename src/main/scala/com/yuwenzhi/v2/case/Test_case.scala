package com.yuwenzhi.v2.`case`

import com.yuwenzhi.v2.`case`.Test_case.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Test}

class Test_case {
  @After
  def close(): Unit ={
    //5.关闭
    sc.stop()
  }

  @Test
  def Test_case(): Unit ={
    val rdd: RDD[String] = sc.textFile("input/agent.log")
    //每省的广告点击top3
    rdd.map(line =>{
      val strings: Array[String] = line.split("\\s+")
      (strings(1)+"-"+strings(4),1)
    }).reduceByKey(_+_).map(prvAndAdr=>{
      val strings: Array[String] = prvAndAdr._1.split("-")
      (strings(0),(strings(1),prvAndAdr._2))
    }).groupByKey().mapValues(t1=>{
      t1.toList.sortWith(_._2 > _._2).take(3)
    }).collect().foreach(println)


  }
}

object Test_case{
  //1.创建SparkConf并设置App名称
  val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

  //2.创建SparkContext，该对象是提交Spark App的入口
  val sc: SparkContext = new SparkContext(conf)

}
