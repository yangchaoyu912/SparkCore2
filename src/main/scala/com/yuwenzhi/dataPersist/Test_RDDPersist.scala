package com.yuwenzhi.dataPersist

import com.yuwenzhi.dataPersist.Test_RDDPersist.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Test}

class Test_RDDPersist {
    @After
    def close(): Unit ={
      //5.关闭
      sc.stop()
    }
    @Test
    def Test_cache01(): Unit ={
      val wordRdd: RDD[(String, Int)] = sc.textFile("input").flatMap(_.split(" "))
        .map({
          word => {
            println("****************")
            (word, 1)
          }
        }).reduceByKey(_ + _)
      println(wordRdd.toDebugString)
      //数据缓存，懒加载，直到触发action时，rdd会被缓存到计算节点的内存中
      //wordRdd.cache()
      println("-----------")

      //wordRdd.persist(StorageLevel.MEMORY_AND_DISK_2).collect()

      println("--------------")
      println(wordRdd.collect())

      println(wordRdd.toDebugString)
    }
}
object Test_RDDPersist{
  //1.创建SparkConf并设置App名称
  val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

  //2.创建SparkContext，该对象是提交Spark App的入口
  val sc: SparkContext = new SparkContext(conf)

}
