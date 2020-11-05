package com.yuwenzhi.v1.action

import com.yuwenzhi.v1.action.Test_action_operator.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Test}

/**
 * test connection
 */
class Test_action_operator {

  @After
  def close(): Unit ={
    //5.关闭ggg
    sc.stop()
  }

  @Test
  def Test_reduce(): Unit ={
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val reduceResult: Int = rdd.reduce(_+_)
    println(reduceResult)
  }

  @Test
  def Test_collect(): Unit ={
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val ints: Array[Int] = rdd.collect()
    println(ints.mkString(", "))
  }

  @Test
  def Test_count(): Unit ={
    //返回rdd中的元素个数
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    println(rdd.count())
  }

  @Test
  def Test_first(): Unit ={
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    println(rdd.first())
  }

  @Test
  def Test_take(): Unit ={
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val ints: Array[Int] = rdd.take(1)
    println(ints.mkString(" "))
  }

  @Test
  def Test_takeOrdered(): Unit ={
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val ints: Array[Int] = rdd.takeOrdered(1)(Ordering[Int].reverse)
    println(ints.mkString(", "))
  }

  @Test
  def Test_aggregate(): Unit ={
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),4)
    val aggregateInt: Int = rdd.aggregate(0)(_+_,_+_)
    val aggregateInt2: Int = rdd.aggregate(10)(_+_,_+_)
    println(rdd.fold(10)(_ + _))
    println(aggregateInt)
    println(aggregateInt2)
  }

  @Test
  def Test_countByKey(): Unit ={
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("a",3)),4)
    val stringToLong: collection.Map[String, Long] = rdd.countByKey()
    println(stringToLong)

  }


  @Test
  def Test_save(): Unit ={
    //save相关的行动算子，有三个：
    // 1. saveAsTextFile(patn): 保存成Text文件

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    //保存成Text文件
    rdd.saveAsTextFile("outputTextFile")

    //序列化成对象然后保存在文件中
    rdd.saveAsObjectFile("outputObjectFile")

    //保存成SequenceFile文件: 注意只有 kv 类型的 rdd 才有 saveAsSequenceFile Action算子
    rdd.map((_,1)).saveAsSequenceFile("outputSeqFile")


    //5.关闭
    sc.stop()
  }

  @Test
  def Test_foreach(): Unit ={
    //1. rdd.collect().foreach(println)  与 2. rdd.foreach(println)的区别：
    //1. driver端把数据发送到Executor端，collect() 行动算子将结果返回driver端
    //所以rdd.collect().foreach(println) 是在driver执行的
    //2. 把数据发送到executor端，在executor端执行打印操作
    val rdd1: RDD[Int] = Test_action_operator.sc.makeRDD(List(1,2,3,4),2)

    rdd1.foreach(println)               //这里的foreach()是spark中 rdd 的行动算子
    //rdd1.collect().foreach(println)     //这里的foreach()是scala中的函数

  }

}

object Test_action_operator{
  //1.创建SparkConf并设置App名称
  val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

  //2.创建SparkContext，该对象是提交Spark App的入口
  val sc: SparkContext = new SparkContext(conf)

}
