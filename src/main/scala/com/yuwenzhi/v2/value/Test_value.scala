package com.yuwenzhi.v2.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Test}

class Test_value() {

  /**
   * value类型的Rdd算子有13种：
   * map()
   * flatMap()
   * mapPartitions()
   * mapPartitionsWithIndex()
   * groupBy()
   * distinct()
   * simple()
   * filter()
   * sortBy()
   * repartition()
   * coalesce()
   * glom()
   * pipe()
   *
   */



  //1. map() 映射
  @Test
  def Test01_map(): Unit ={
    val rdd: RDD[Int] = Test_value.sc.makeRDD(1 to 6,1)
    rdd.map(_*2).collect().foreach(println)
  }

  //2. flatMap() 扁平化+映射
  @Test
  def Test02_flatMap(): Unit ={
    Test_value.sc.makeRDD(List(Array(1,2,3),Array(3,4,5)))
      .flatMap(arr=>arr.map(_*2))
      .collect().foreach(println)
  }



  @After
  def close(): Unit ={
    Test_value.sc.stop()
  }


}

object Test_value{
  // 解决scala中类里面不能定义静态成员属性的问题：
  // 在伴生对象中定义属性，在伴生类中直接以Test_value.sc的形式使用
  val sc:SparkContext =
    new SparkContext(new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]"))
}
