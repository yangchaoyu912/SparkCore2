package com.yuwenzhi.v1.`case`

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class AdvertisingTopN {

  @Test
  /**
   * 需求： 求每个省的广告点击top3
   */
  def advertClickTop3(): Unit ={

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input")
    //时间戳，省份，城市，用户，广告
    //(prv,adv) -> (prv-adv,1)) ->
    val prvToAdvCli: RDD[(String, (String, Int))] = rdd.map(line => {
      val lineArr: Array[String] = line.split("\\s+")
      (lineArr(1) + "-" + lineArr(4), 1)
    }).reduceByKey(_ + _).map(prvAndAdv => {
      val paArr: Array[String] = prvAndAdv._1.split("-")
      (paArr(0), (paArr(1), prvAndAdv._2))
    })

    //分组
    val prvToAdvCliAfterGroup: RDD[(String, Iterable[(String, Int)])] = prvToAdvCli.groupByKey()
    // (河北，compactBuffer(（AAA,1),(BBB,2)))
    // 对value排序后取top3
    prvToAdvCliAfterGroup.mapValues(
      _.toList.sortWith(_._2 > _._2).take(3)
    ).collect().foreach(println)

    //5.关闭
    sc.stop()
  }
}
