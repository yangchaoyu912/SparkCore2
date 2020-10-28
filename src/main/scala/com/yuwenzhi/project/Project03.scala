package com.yuwenzhi.project

import com.yuwenzhi.project.Project03.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Test}

import scala.collection.mutable.{ArrayOps, ListBuffer}
import scala.collection.{immutable, mutable}

class Project03 {

  @After
  def close(): Unit ={
    //5.关闭
    sc.stop()
  }

  @Test
  def test_Method4(): Unit ={
    //通过样例类和reduceByKey()
    //通过累加器的方式
    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")
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
      })
    val flatRdd: RDD[(String, CategoryCountInfo)] = actionRdd.flatMap {
      case action: UserVisitAction => {
        if (action.click_category_id != -1) {
          List((action.click_category_id.toString, CategoryCountInfo(action.click_category_id.toString, 1, 0, 0)))
        } else if (action.order_category_ids != "null") {
          val result = new ListBuffer[(String, CategoryCountInfo)]
          val ids: ArrayOps.ofRef[String] = action.order_category_ids.split(",")
          for (elem <- ids) {
            result.append((elem, CategoryCountInfo(elem, 0, 1, 0)))
          }
          result
        } else if (action.pay_category_ids != "null") {
          val result = new ListBuffer[(String, CategoryCountInfo)]
          val ids: ArrayOps.ofRef[String] = action.pay_category_ids.split(",")
          for (elem <- ids) {
            result.append((elem, CategoryCountInfo(elem, 0, 0, 1)))
          }
          result
        } else {
          Nil
        }
      }
      case _ => {
        Nil
      }
    }
    val reduceRdd: RDD[(String, CategoryCountInfo)] = flatRdd.reduceByKey {
      case (accInfo, next) => {
        accInfo.clickCount = accInfo.clickCount + next.clickCount
        accInfo.orderCount = accInfo.orderCount + next.orderCount
        accInfo.payCount = accInfo.payCount + next.payCount
        accInfo
      }
    }
    val resultRdd: RDD[CategoryCountInfo] = reduceRdd.map(_._2).sortBy(info => {
      (info.clickCount, info.orderCount, info.payCount)
    }, false)
    resultRdd.take(10).foreach(println)
  }

  @Test
  def test_Method5(): Unit ={
    //通过累加器的方式
    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")
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
      })
    val acc = new CategoryCountAccumulator
    sc.register(acc,"sum")
    actionRdd.foreach(acc.add)
    val accResult: mutable.Map[(String, String), Long] = acc.value
    //结构转化 => CategoryCountInfo
    val groupMap: Map[String, mutable.Map[(String, String), Long]] = accResult.groupBy(_._1._1)
    val result: immutable.Iterable[CategoryCountInfo] = groupMap.map {
      case (id, map) => {
        val click: Long = map.getOrElse((id, "click"), 0L)
        val order: Long = map.getOrElse((id, "order"), 0L)
        val pay: Long = map.getOrElse((id, "pay"), 0L)
        CategoryCountInfo(id, click, order, pay)
      }
    }
    val infos: List[CategoryCountInfo] = result.toList.sortWith {
      case (info1, info2) => {
        if (info1.clickCount == info2.clickCount) {
          if (info1.orderCount == info2.orderCount) {
            if (info1.payCount == info2.payCount) {
              true
            } else {
              info1.payCount > info2.payCount
            }
          } else {
            info1.orderCount > info2.orderCount
          }
        } else {
          info1.clickCount > info2.clickCount
        }
      }
    }
    infos.take(10).foreach(println)

  }

}
//((鞋，click),1)
class CategoryCountAccumulator extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]]{

  private val map: mutable.Map[(String, String), Long] = mutable.Map[(String,String),Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new CategoryCountAccumulator

  override def reset(): Unit = map.clear()

  override def add(action: UserVisitAction): Unit = {
    if(action.click_category_id != -1){
      val key = (action.click_category_id.toString,"click")
      map(key) = map.getOrElse(key,0L) + 1
    }else if(action.order_category_ids != "null"){
      val ids: Array[String] = action.order_category_ids.split(",")
      for (elem <- ids) {
        val key = (elem,"order")
        map(key) = map.getOrElse(key,0L) + 1
      }
    }else if(action.pay_category_ids != "null"){
      val ids: Array[String] = action.pay_category_ids.split(",")
      for (elem <- ids) {
        val key = (elem,"pay")
        map(key) = map.getOrElse(key,0L) + 1
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    other.value.foreach{
      case (k,v)=>{
        map(k) = map.getOrElse(k,0L) + v
      }
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}

object Project03{
  //1.创建SparkConf并设置App名称
  val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

  //2.创建SparkContext，该对象是提交Spark App的入口
  val sc: SparkContext = new SparkContext(conf)

}
