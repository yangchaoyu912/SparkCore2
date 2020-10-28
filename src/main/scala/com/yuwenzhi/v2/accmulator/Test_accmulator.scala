package com.yuwenzhi.v2.accmulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Test_accmulator {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("a",4)))
    //使用spark自带的常用累加器
    val sum1: LongAccumulator = sc.longAccumulator("sum1")
    rdd.foreach{
      case(a,count)=>{
        sum1.add(count)
      }
    }
    println(sum1.value)

    //5.关闭
    sc.stop()
  }
}
