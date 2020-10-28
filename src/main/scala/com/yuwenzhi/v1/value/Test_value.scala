package com.yuwenzhi.v1.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class Test_value {
  @Test
  def Test01_Map(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    sc.makeRDD(1 to 4,3).map(_ * 2).foreach( e => print(e + "\t"))

    //5.关闭
    sc.stop()
  }

  //mapPartitions()以分区为单位执行map
  //map() 与 mapPartition()方法的区别
  //map() 一次处理 一个元素，而mapPartitions()一次处理一个分区的数据
  @Test
  def test_mapPartitions(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    sc.makeRDD(1 to 4,2).mapPartitions(p => p.map(_* 2)).foreach(print)

    //5.关闭
    sc.stop()
  }

  @Test
  def Test_mapPartitionWithIndex(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    sc.makeRDD(1 to 4,2).mapPartitionsWithIndex((index,pItem)=>{
      pItem.map((index,_))
    }).foreach(print)

    //5.关闭
    sc.stop()
  }

  @Test
  def Test_flatMap(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    sc.makeRDD(List(List(1,2),List(3,4)),2).flatMap(x=>x).foreach(println)

    //5.关闭
    sc.stop()
  }

  @Test
  def Test_glom(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[Array[Int]] = sc.makeRDD(1 to 4, 2).glom()

    val value1: RDD[Int] = value.map(_.max)

    value1.foreach(println)

    //5.关闭
    sc.stop()
  }

  @Test
  def Test_groupBy_WordCount(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

     val value: RDD[(String, Iterable[(String, Int)])] = sc.makeRDD(List("hello scala", "hello spark", "hello flink", "hello flume", "hello kafka"))
      .flatMap(_.split(" ")) map((_,1)) groupBy(_._1)
    value.map(kv=>(kv._1,kv._2.size)).foreach(print)

    //5.关闭
    sc.stop()
  }

  @Test
  def Test_filter(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    sc.makeRDD(1 to 4,2).filter(_ % 2 == 0).foreach(print)

    //5.关闭
    sc.stop()
  }

  //coalesce() : 合并分区
  @Test
  def Test_coalesce(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 6 , 6)
    // 不执行shuffle过程
    val coalesceRdd: RDD[Int] = rdd.coalesce(3)

    //打印查看对应分区数据
    coalesceRdd.mapPartitionsWithIndex((index,datas)=>{
      datas.foreach(data=>println(index + "=>" + data))
      datas   //注意：返回分区数据
    }).collect()

    //执行shuffle过程
    val rdd2: RDD[Int] = sc.makeRDD(1 to 6 , 6)
    rdd2.coalesce(3,true).mapPartitionsWithIndex((index,datas) =>{
      datas.foreach(data=>println(index + "=>" + data))
      datas
    }).collect()

    //5.关闭
    sc.stop()
  }

  //coalesce()和repartition()的区别
  //repartition(): 一般用于扩大分区 repartition()底层调用coalesce()  进行shuffle过程
  //对于扩大分区： 如果不进行shuffle过程，没有意义。增加的那些分区中没有数据
  //coalesce(): 一般为缩减分区,(合并分区）
  //分区原理核心源码：
  /**
   * 走shuffle:
   * // i : 分区号      prev.partitions.length: 上一个Rdd的分区数    maxPartitions ：要分几个区
   * val rangeStart = ((i.toLong * prev.partitions.length) / maxPartitions).toInt
   * val rangeEnd = (((i.toLong + 1) * prev.partitions.length) / maxPartitions).toInt
   * (rangeStart until rangeEnd).foreach{ j => groupArr(i).partitions += prev.partitions(j)
   *
   * start: (分区号 * 上一个Rdd的分区数） / 当前分区数
   * end : （(分区号 + 1） * 上一个Rdd的分区数 ） / 当前分区数
   *
   */
    @Test
  def Test_repartition(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,7,8,9,10),4)

    // 使用repartition()重新分区， 这里进行扩大分区
    rdd.repartition(8).mapPartitionsWithIndex(
        //打印分区号并打印分区中的内容
        (index,PItems)=>{
          println(index + " | " + PItems.mkString(", "))
          PItems.map((index,_))
        }).collect()

    //5.关闭
    sc.stop()
  }

  @Test
  def Test_sortBy(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val strRdd: RDD[String] = sc.makeRDD(List("1", "22", "12", "2", "3"))

    strRdd.sortBy(_.toInt).collect().foreach(println)

    //5.关闭
    sc.stop()

  }

  //pipe() 调用脚本
  @Test
  def Test_pipe(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("hello","how","are","you"),1)

    rdd.pipe("input/pipeTest.sh").collect()

    //5.关闭
    sc.stop()
  }

}
