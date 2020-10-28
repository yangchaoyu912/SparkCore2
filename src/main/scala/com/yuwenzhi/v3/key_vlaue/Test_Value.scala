package com.yuwenzhi.v3.key_vlaue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Test}

class Test_Value {

  @After
  def close(): Unit ={
    Test_Value.sc.stop()
  }

  @Test
  def Test_map(): Unit ={
    val rdd: RDD[Int] = Test_Value.sc.makeRDD(List(1,2,3,4),1)
    rdd.map(_*2).collect().foreach(println)
  }

  @Test
  def Test_flatMap(): Unit ={
    val rdd: RDD[String] = Test_Value.sc.makeRDD(List("hello word","hello scala"),2)
    rdd.flatMap(_.split(" ")).groupBy(a=>a).map((data)=>(data._1,data._2.size)).collect().foreach(println)
  }

  @Test
  def Test_mapPartitions(): Unit ={
    val rdd: RDD[Int] = Test_Value.sc.makeRDD(List(1,2,3,4),2)
    rdd.mapPartitions(datas=>datas.map(_*2)).collect().foreach(println)
  }

  @Test
  def Test_mapPartitionsWithIndex(): Unit ={
    val rdd: RDD[Int] = Test_Value.sc.makeRDD(List(1,2,3,4),4)
    rdd.mapPartitionsWithIndex((index,pitem)=>{
      pitem.map((index,_))
    }).collect().foreach(println)
  }

  @Test
  def Test_filter(): Unit ={
    val rdd: RDD[Int] = Test_Value.sc.makeRDD(List(1,2,3,4),1)
    rdd.filter(_%2==0).collect().foreach(println)
  }

  @Test
  def Test_simple(): Unit ={
    val rdd: RDD[Int] = Test_Value.sc.makeRDD(List(1,2,3,4),1)
  }

  @Test
  def Test_groupBy(): Unit ={
    val rdd: RDD[Int] = Test_Value.sc.makeRDD(List(1,2,3,4),1)
    rdd.groupBy(_%2).collect().foreach(println)
  }

  @Test
  def Test_distinct(): Unit ={
    val rdd: RDD[Int] = Test_Value.sc.makeRDD(List(1,2,3,4,3,5,1,6),1)
    rdd.distinct().collect().foreach(println)
  }

  @Test
  def Test_coalesce(): Unit ={
    //合并分区
    val rdd: RDD[Int] = Test_Value.sc.makeRDD(List(1,2,3,4),4)
    rdd.coalesce(2).collect().foreach(println)
  }

  @Test
  def Test_repartition(): Unit ={
    //扩大分区
    val rdd: RDD[Int] = Test_Value.sc.makeRDD(List(1,2,3,4),1)
    rdd.repartition(4).collect().foreach(println)
  }

  @Test
  def Test_sortBy(): Unit ={
    val rdd: RDD[Int] = Test_Value.sc.makeRDD(List(1,2,3,4),1)
    rdd.sortBy(a=>a,false).collect().foreach(println)
  }

  @Test
  def Test_glom(): Unit ={
    //分区变数组
    val rdd: RDD[Int] = Test_Value.sc.makeRDD(List(1,2,3,4),4)
    rdd.glom().collect().foreach(println)
  }


}

object Test_Value {
  //2.创建SparkContext，该对象是提交Spark App的入口
  val sc: SparkContext = new SparkContext(new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]"))

}
