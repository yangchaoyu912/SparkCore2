package com.yuwenzhi.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object serializable01_object {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val user1 = new User()
    user1.name="lisi"

    val user2 = new User()
    user2.name="wangyu"

    val userRdd: RDD[User] = sc.makeRDD(List(user1,user2))
    //初始化工作在Driver端进行，实际运行程序在Executor端，这就涉及到进程间通信
    //数据从driver端到executor端，需要序列化

    //ERROR报java.io.NotSerializableException
    //userRdd.foreach(user=>println(user.name))

    //数据为空时，可以正确执行，没有报异常
    val userRdd2: RDD[User] = sc.makeRDD(List())
    //userRdd2.foreach(user=>println(user.name))
    //如果想数据再executor正确进行处理，需要让类继承serializable
    userRdd.foreach(user=>println(user.name))

    //5.关闭
    sc.stop()
  }
}

class User extends Serializable {
  var name:String=_

}
