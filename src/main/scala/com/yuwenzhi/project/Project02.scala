package com.yuwenzhi.project

import com.yuwenzhi.project.Project02.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Test}

import scala.collection.{immutable, mutable}

class Project02 {
  @After
  def close(): Unit ={
    //5.关闭
    sc.stop()
  }

  //累加器的方式实现
  @Test
  def test_Accumulator(): Unit ={
    //map: ((id,click),3) ((id,order),4) ((id,pay),5)
    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")
    //1. 将数据封装到UserVisitAction对象中
    val actionRdd: RDD[UserVisitAction] = rdd.map(
      line => {
        val infos: Array[String] = line.split("_")
        UserVisitAction(
          infos(0),
          infos(1).toLong,
          infos(2),
          infos(3).toLong,
          infos(4),
          infos(5),
          infos(6).toLong,
          infos(7).toLong,
          infos(8),
          infos(9),
          infos(10),
          infos(11),
          infos(12).toLong
        )
      }
    )
    //创建累加器对象
    val acc = new CategoryCountAccumulator2
    //注册累加器
    sc.register(acc)
    //使用
    actionRdd.foreach(acc.add)
    //获取结果
    val accResult: mutable.Map[(String, String), Long] = acc.value
    //按照品类id 分组
    val groupMap: Map[String, mutable.Map[(String, String), Long]] = accResult.groupBy(_._1._1)
    //结构转换，将点击次数，订单次数，支付次数封装到 CategoryCountInfo对象中
    val cateCountInfo: immutable.Iterable[CategoryCountInfo] = groupMap.map {
      case (id, allInfoMapById) => {
        val click: Long = allInfoMapById.getOrElse((id, "click"), 0L)
        val order: Long = allInfoMapById.getOrElse((id, "order"), 0L)
        val pay: Long = allInfoMapById.getOrElse((id, "pay"), 0L)
        CategoryCountInfo(id, click, order, pay)
      }
    }
    val result: List[CategoryCountInfo] = cateCountInfo.toList.sortWith {
      case (left, right) => {
        if (left.clickCount == right.clickCount) {
          if (left.orderCount == right.orderCount) {
            if (left.payCount == right.payCount) {
              true
            } else {
              left.payCount > right.payCount
            }
          } else {
            left.orderCount > right.orderCount
          }
        } else {
          left.clickCount > right.clickCount
        }
      }
    }.take(10)
    result.foreach(println)

  }
}

class CategoryCountAccumulator2 extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]] {

  private val resultMap: mutable.Map[(String, String), Long] = mutable.Map[(String,String),Long]()

  override def isZero: Boolean = resultMap.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new CategoryCountAccumulator2

  override def reset(): Unit = resultMap.clear()

  override def add(action: UserVisitAction): Unit = {
    if(action.click_category_id != -1){
      val key = (action.click_category_id.toString,"click")
      resultMap(key) = resultMap.getOrElse(key,0L) + 1
    }else if(action.order_category_ids != "null"){
      val ids: Array[String] = action.order_category_ids.split(",")
      ids.foreach(id=>{
        val key = (id.toString,"order")
        resultMap(key) = resultMap.getOrElse(key,0L) + 1
      })
    }else if(action.pay_category_ids != "null"){
      val ids: Array[String] = action.pay_category_ids.split(",")
      ids.foreach(id=>{
        val key = (id.toString,"pay")
        resultMap(key) = resultMap.getOrElse(key,0L) + 1
      })
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    other.value.foreach{
      case(k,v)=>{
        resultMap(k) = resultMap.getOrElse(k,0L) + v
      }
    }
  }

  override def value: mutable.Map[(String, String), Long] = resultMap
}

object Project02{
  //1.创建SparkConf并设置App名称
  val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

  //2.创建SparkContext，该对象是提交Spark App的入口
  val sc: SparkContext = new SparkContext(conf)

}


