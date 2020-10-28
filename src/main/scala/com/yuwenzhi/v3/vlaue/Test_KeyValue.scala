package com.yuwenzhi.v3.vlaue

import com.yuwenzhi.v3.key_vlaue.Test_Value
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.junit.{After, Test}

class Test_KeyValue {
  @After
  def close(): Unit ={
    Test_Value.sc.stop()
  }

  @Test
  //根据key 重新分区
  def Test_partitionsBy(): Unit ={
    val rdd: RDD[(String, Int)] = Test_Value.sc.makeRDD(List(("a",1),("a",2),("b",1)),1)
    rdd.partitionBy(new HashPartitioner(2)).collect().foreach(println)

    rdd.partitionBy(new MyPartitioner(2)).collect().foreach(println)
  }

  @Test
  def Test_groupByKey(): Unit ={
    val rdd: RDD[(String, Int)] = Test_Value.sc.makeRDD(List(("a",1),("a",2),("b",1)),1)
    rdd.groupByKey().collect().foreach(println)
  }

  @Test
  def Test_reduceByKey(): Unit ={
    val rdd: RDD[(String, Int)] = Test_Value.sc.makeRDD(List(("a",1),("a",2),("b",1)),1)
    rdd.reduceByKey(_+_).collect().foreach(println)
  }

  @Test
  def Test_aggregateByKey(): Unit ={
    val rdd: RDD[(String, Int)] = Test_Value.sc.makeRDD(List(("a",1),("a",2),("b",1),("c",2),("b",3)),3)
    //需求： 分区内key对应的最大值，分区间key对应的最大值求和
    rdd.aggregateByKey(0)((intval,nextVal)=>math.max(intval,nextVal),_+_).collect().foreach(println)
  }

  @Test
  def Test_combineByKey(): Unit ={
    val rdd: RDD[(String, Int)] = Test_Value.sc.makeRDD(List(("a",1),("a",2),("b",1),("c",2),("b",3)),3)
    //需求： 求每个分区内相同key对应值的总和，以及总数据中key对应值的均值
    val rdd1: RDD[(String, (Int, Int))] = rdd.combineByKey((_, 1), (acc: (Int, Int), nextVal) => {
      (acc._1 + nextVal, acc._2 + 1)
    }, (t1: (Int, Int), t2: (Int, Int)) => {
      (t1._1 + t2._1, t1._2 + t2._2)
    })
    rdd1.map(kv=>{
      (kv._1,kv._2._1 / kv._2._2 toDouble)
    }).collect().foreach(println)
  }

  @Test
  def Test_sortByKey(): Unit ={
    val rdd: RDD[(String, Int)]=Test_Value.sc.makeRDD(List(("a",1),("a",2),("b",1),("c",2),("b",3)),3)
    rdd.sortByKey(false).collect().foreach(println)
  }

  @Test
  def Test_join(): Unit ={
    val rdd: RDD[(String, Int)]=Test_Value.sc.makeRDD(List(("a",1),("a",2),("b",1),("c",2),("b",3)),3)
    val rdd2: RDD[(String, Int)]=Test_Value.sc.makeRDD(List(("a",1),("a",2),("b",1),("c",2),("b",3)),3)
    rdd.join(rdd2).collect().foreach(println)
  }

  @Test
  def Test_cogroup(): Unit ={
    val rdd: RDD[(String, Int)]=Test_Value.sc.makeRDD(List(("ac",1),("ab",2),("b",1),("c",2),("b",3)),3)
    val rdd2: RDD[(String, Int)]=Test_Value.sc.makeRDD(List(("a",1),("a",2),("b",1),("c",2),("b",3)),3)
    rdd.join(rdd2).collect().foreach(println)
  }

  @Test
  def Test_mapValues(): Unit ={
    val rdd: RDD[(String, Int)]=Test_Value.sc.makeRDD(List(("a",1),("a",2),("b",1),("c",2),("b",3)),3)
    rdd.mapValues(_+"-v").collect().foreach(println)
  }

}

class MyPartitioner(num:Int) extends Partitioner{
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    if(key.isInstanceOf[Int]){
      val keyInt: Int = key.asInstanceOf[Int]
      if(keyInt % 2==0) 0 else 1
    }else{
      0
    }
  }
}

object Test_KeyValue{
  //2.创建SparkContext，该对象是提交Spark App的入口
  val sc: SparkContext = new SparkContext(new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]"))
}
