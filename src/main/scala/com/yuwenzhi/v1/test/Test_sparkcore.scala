package com.yuwenzhi.v1.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class Test_sparkcore {

//  6、需求说明：创建一个RDD，使每个元素跟所在分区号形成一个元组，组成一个新的RDD
    @Test
    def Test01(): Unit ={
      //1.创建SparkConf并设置App名称
      val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

      //2.创建SparkContext，该对象是提交Spark App的入口
      val sc: SparkContext = new SparkContext(conf)

      sc.makeRDD(Array(1,2,3,4),2).mapPartitionsWithIndex((index,item)=>{
        item.map((index,_))
      }).foreach(println)

      //5.关闭
      sc.stop()
    }
//  7、需求说明：创建一个2个分区的RDD，并将每个分区的数据放到一个数组，求出每个分区的最大值
    @Test
    def Test02(): Unit ={
      //1.创建SparkConf并设置App名称
      val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

      //2.创建SparkContext，该对象是提交Spark App的入口
      val sc: SparkContext = new SparkContext(conf)

      sc.makeRDD(Array(1,2,3,4),2).mapPartitions(pItems =>{
        println(pItems.toArray.max)
        pItems
      }).collect()

      //5.关闭
      sc.stop()
    }
//  8、需求说明：创建一个RDD，按照元素模以2的值进行分组。
    @Test
    def test03(): Unit ={
      //1.创建SparkConf并设置App名称
      val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

      //2.创建SparkContext，该对象是提交Spark App的入口
      val sc: SparkContext = new SparkContext(conf)

      sc.makeRDD(Array(1,2,3,4)).groupBy(_ % 2 == 0).collect().foreach(println)

      //5.关闭
      sc.stop()
    }
//  9、需求说明：创建一个RDD，过滤出对2取余等于0的数据
    @Test
    def test04(): Unit ={
      //1.创建SparkConf并设置App名称
      val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

      //2.创建SparkContext，该对象是提交Spark App的入口
      val sc: SparkContext = new SparkContext(conf)

      println(sc.makeRDD(Array(1, 2, 3, 4)).filter(_ % 2 == 0).collect())

      //5.关闭
      sc.stop()
    }
//  10、需求说明：创建一个RDD（1-10），从中选择放回和不放回抽样
    @Test
    def test05(): Unit ={
      //1.创建SparkConf并设置App名称
      val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

      //2.创建SparkContext，该对象是提交Spark App的入口
      val sc: SparkContext = new SparkContext(conf)
      // 不放回抽样
      sc.makeRDD(1 to 10).sample(false,0.5,10).collect().foreach(println)
      println("------------")
      //放回抽样
      //fraction: 期望出现的次数
      sc.makeRDD( 1 to 10).sample(true,2).collect.foreach(println)
      //5.关闭
      sc.stop()
    }
//  11、distinct()去重源码解析（自己写算子实现去重）
    @Test
    def test06(): Unit ={
      //1.创建SparkConf并设置App名称
      val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

      //2.创建SparkContext，该对象是提交Spark App的入口
      val sc: SparkContext = new SparkContext(conf)

      val value: RDD[(Int, Iterable[(Int, Null)])] = sc.makeRDD(Array(1,2,2,1,3,4,4,6)).map((_,null)).groupBy(_._1)
      value.map(a=>(a._1,1)).foreach(print)


      //5.关闭
      sc.stop()
    }


//  12、用textFile、map、flatmap、groupBy实现wordcount
    @Test
    def test07(): Unit ={
      //1.创建SparkConf并设置App名称
      val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

      //2.创建SparkContext，该对象是提交Spark App的入口
      val sc: SparkContext = new SparkContext(conf)

      sc.textFile("input")
        .flatMap(_.split(" "))
        .groupBy(word=>word)
        .map(elem => (elem._1,elem._2.size))
        .foreach(println)

      val rdd2: RDD[(String, Int)] = sc.textFile("input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
      rdd2.foreach(println)

      //5.关闭
      sc.stop()
    }

}
