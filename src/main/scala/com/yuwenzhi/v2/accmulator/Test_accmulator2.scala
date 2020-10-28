package com.yuwenzhi.v2.accmulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Test_accmulator2 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3. 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark"),4)

    // 定义一个累加器
    var sumAccmulator = new MyAccmulator
    sc.register(sumAccmulator,"wordCount")

    //4. 需求： 统计以 " H " 开头的单词
    rdd.foreach(word=>{
      sumAccmulator.add(word)
    })

    println(sumAccmulator.value)

    //5.关闭
    sc.stop()
  }
}
class MyAccmulator extends AccumulatorV2[String,mutable.Map[String,Long]]{
  // 定义一个返回的结果Map()
  var map: mutable.Map[String, Long] = mutable.Map[String,Long]()


  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccmulator

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    //Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark"
    if(v.contains("H")){
      //累计每个Executor中以H开头的单词
      map(v) = map.getOrElse(v,0L) + 1
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    // 合并多个executor返回的map
    // 拿到返回的Map
    other.value.foreach{
      case (word,count)=>{
        map(word) = map.getOrElse(word,0L) + count
      }
    }
  }

  override def value: mutable.Map[String, Long] = map
}


