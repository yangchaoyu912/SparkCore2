package com.yuwenzhi.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Test}

import scala.collection.mutable.{ArrayOps, ListBuffer}
import com.yuwenzhi.project.Project01.sc
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

class Project01 {

  @After
  def close(): Unit ={
    //5.关闭
    sc.stop()
  }

  //需求： 热门品类的Top10   点击数 >> 下单数 >> 支付数
  //最终得到的数据形式为： CategoryCountInfo(品类id,点击次数,订单次数, 支付次数）
  //使用样例类的方式实现：
  @Test
  def test01_case(): Unit = {

    //3. 获取原始数据
    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")
    val uvaRdd: RDD[UserVisitAction] = rdd.map(data => {
      //数据转成对象
      val infos: Array[String] = data.split("_")
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
    //将数据转化为CategoryCountInfo对象
    val categoryCountInfoRdd: RDD[CategoryCountInfo] = uvaRdd.flatMap {
      //先匹配类型
      case act: UserVisitAction => {
        if (act.click_category_id != -1) {
          //返回集合，因为这里用的是flatMap(),map方法处理完，元素为集合
          List(CategoryCountInfo(act.click_category_id.toString, 1, 0, 0))
        } else if (act.order_category_ids != "null") {
          //下单品类id可能有多个
          val infos = new ListBuffer[CategoryCountInfo]
          val ids: ArrayOps.ofRef[String] = act.order_category_ids.split(",")
          for (elem <- ids) {
            infos.append(CategoryCountInfo(elem.toString, 0, 1, 0))
          }
          infos
        } else if (act.pay_category_ids != "null") {
          //支付品类id可能有多个
          val infos = new ListBuffer[CategoryCountInfo]
          val ids: ArrayOps.ofRef[String] = act.pay_category_ids.split(",")
          for (elem <- ids) {
            infos.append(CategoryCountInfo(elem.toString, 0, 0, 1))
          }
          infos
        } else {
          Nil
        }
      }
      case _ => Nil
    }
    //将相同品类的数据分到一组
    val groupRdd: RDD[(String, Iterable[CategoryCountInfo])] = categoryCountInfoRdd.groupBy(info=>info.categoryId)

    //聚合
    val reduceRdd: RDD[(String, CategoryCountInfo)] = groupRdd.mapValues(
      datas => {
          //scala中的reduce聚合
            datas.reduce(
            (acc, next) => {
              acc.clickCount = acc.clickCount + next.clickCount
              acc.orderCount = acc.orderCount + next.orderCount
              acc.payCount = acc.payCount + next.payCount
              acc
            }
        )
      }
    )
    val resultInfos: Array[CategoryCountInfo] = reduceRdd.map(_._2).
      sortBy(info => (info.clickCount, info.orderCount, info.clickCount) //可以传元组
        , false).take(10)
    resultInfos.foreach(println)

  }

  @Test
  def test01_case2(): Unit ={
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
    //2. 数据结构转换，转换成CategoryCountInfo对象 => RDD[categoryCountInfo]
    // 由于一个品类对象id(一个点击次数) ->  多个订单id
    //                               ->  多个支付id，所以map()后应当是List[CategoryCountInfo]类型的数据
    // 然后应当扁平化 => 直接使用flatMap()方法操作
    val faltRdd: RDD[CategoryCountInfo] = actionRdd.flatMap {
      case action: UserVisitAction => {
        //先统计点击数
        if (action.click_category_id != -1) {
          //一个品类对象id就是一个点击次数，所以这里的集合中只有一个元素
          List( CategoryCountInfo(action.click_category_id.toString, 1, 0, 0))
        } else if (action.order_category_ids != "null") {
          //再统计订单次数
          //一个品类对象中有多个订单id，多个元素
          val infos = new ListBuffer[CategoryCountInfo]
          val orderIds: ArrayOps.ofRef[String] = action.order_category_ids.split(",")
          for (elem <- orderIds) {
            infos.append( CategoryCountInfo(elem.toString, 0, 1, 0))
          }
          infos
        } else if (action.pay_category_ids != "null") {
          //最后统计支付次数
          //一个品类对象中有多个支付id，多个元素
          val infos = new ListBuffer[CategoryCountInfo]
          val orderIds: ArrayOps.ofRef[String] = action.pay_category_ids.split(",")
          for (elem <- orderIds) {
            infos.append( CategoryCountInfo(elem.toString, 0, 0, 1))
          }
          infos
        } else {
          Nil
        }
      }
      case _ => {
        Nil
      }
    }
    //RDD[CategoryCountInfo]   :   聚合之前，需要进行分组
    val groupRdd: RDD[(String, Iterable[CategoryCountInfo])] = faltRdd.groupBy(_.categoryId)
    groupRdd.foreach(println)
    //对groupRdd中的value进行聚合
    val reduceRdd: RDD[(String, CategoryCountInfo)] = groupRdd.mapValues(
      cateCountInfos => {
        //聚合，driver端聚合
        cateCountInfos.reduce((acc, next) => {
          acc.clickCount = acc.clickCount + next.clickCount
          acc.orderCount = acc.orderCount + next.orderCount
          acc.payCount = acc.payCount + next.payCount
          acc
        })
    })
    //去除key
    val mapRdd: RDD[CategoryCountInfo] = reduceRdd.map(_._2)
    //根据点击数，订单数，支付数进行排序，得出 热门品类 Top10
    val top10: Array[CategoryCountInfo] = mapRdd.sortBy(cateCountInfo => {
      //传个元组
      (cateCountInfo.clickCount, cateCountInfo.orderCount, cateCountInfo.payCount)
    }, false).take(10)
    top10.foreach(println)
  }

  // 样例类 + 算子优化
  // 说明： 上面的方案中使用groupBy() , 没有提前聚合的功能（分区内），使用reduceByKey()算子
  @Test
  def test01_caseClassAndOptimize(): Unit ={
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
    //RDD[UserVisitAction] 结构转化
    //为了优化，不使用groupBy()，使用reduceByKey()
    //需要将 RDD[UserVisitAction] => RDD[(cate_id,UserVisitAction)] K-V类型
    val flatMapRdd: RDD[(String, CategoryCountInfo)] = actionRdd.flatMap {
      case action: UserVisitAction => {
        if (action.click_category_id != -1) {
          List((action.click_category_id.toString, CategoryCountInfo(action.click_category_id.toString, 1, 0, 0)))
        } else if (action.order_category_ids != "null") {
          val ids: ArrayOps.ofRef[String] = action.order_category_ids.split(",")
          val resultList = new ListBuffer[(String, CategoryCountInfo)]
          for (elem <- ids) {
            resultList.append((elem, CategoryCountInfo(elem, 0, 1, 0)))
          }
          resultList
        } else if (action.pay_category_ids != "null") {
          val ids: ArrayOps.ofRef[String] = action.pay_category_ids.split(",")
          val resultList = new ListBuffer[(String, CategoryCountInfo)]
          for (elem <- ids) {
            resultList.append((elem, CategoryCountInfo(elem, 0, 0, 1)))
          }
          resultList
        } else {
          Nil
        }
      }
      case _ => {
        Nil
      }
    }
    //聚合
    val reduceRdd: RDD[(String, CategoryCountInfo)] = flatMapRdd.reduceByKey((accInfo, next) => {
      accInfo.clickCount = accInfo.clickCount + next.clickCount
      accInfo.payCount = accInfo.payCount + next.payCount
      accInfo.orderCount = accInfo.orderCount + next.orderCount
      accInfo
    })
    val result: Array[CategoryCountInfo] = reduceRdd.map(_._2).sortBy(info => {
      (info.clickCount, info.orderCount, info.payCount)
    }, false).take(10)
    result.foreach(println)
  }

  @Test
  def test_CategoryCountAccumulator(): Unit ={
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
    val acc = new CategoryCountAccumulator
    //注册累加器
    sc.register(acc,"sum")
    //使用
    actionRdd.foreach(acc.add)
    //获取结果
    val result: mutable.Map[(String, String), Long] = acc.value
    //此时程序运行在Driver端
    //分组，聚合
    val groupMap: Map[String, mutable.Map[(String, String), Long]] = result.groupBy(_._1._1)
    //结构转化 为 CategoryCountInfo对象（cate_id，clickCount,orderCount,payCount)
    //   ==> Iterable[CategoryCountInfo]
    val cateCountInfo: immutable.Iterable[CategoryCountInfo] = groupMap.map {
      case (id, map) => {
        val click: Long = map.getOrElse((id, "click"), 0L)
        val order: Long = map.getOrElse((id, "order"), 0L)
        val pay: Long = map.getOrElse((id, "pay"), 0L)
        CategoryCountInfo(id, click, order, pay)
      }
    }
    val resultList: List[CategoryCountInfo] = cateCountInfo.toList.sortWith(
      (left, right) => {
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
      }).take(10)
    resultList.foreach(println)
  }

}

object Project01{
  //1.创建SparkConf并设置App名称
  val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

  //2.创建SparkContext，该对象是提交Spark App的入口
  val sc: SparkContext = new SparkContext(conf)
}

/**
 * //品类行为统计累加器
 * // ((鞋,click),1)
 * // ((鞋,order),1)
 * // ((鞋,pay),1)
 * // ((衣服,pay),1)
 */
class CategoryCountAccumulator extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]]{

  val map: mutable.Map[(String, String), Long] = mutable.Map[(String,String),Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new CategoryCountAccumulator

  override def reset(): Unit = map.clear()

  override def add(v: UserVisitAction): Unit = {
    if(v.click_category_id != -1){
      //((鞋,click),1)
      val key = (v.click_category_id.toString,"click")
      map(key) = map.getOrElse(key,0L) + 1L
    }else if(v.order_category_ids != "null"){
      val ids: Array[String] = v.order_category_ids.split(",")
      for (elem <- ids) {
          val key = (elem.toString,"order")
          map(key) = map.getOrElse(key,0L) +  1L
      }
    }else if(v.pay_category_ids != "null"){
      val ids: Array[String] = v.pay_category_ids.split(",")
      for (elem <- ids) {
        val key = (elem.toString,"pay")
        map(key) = map.getOrElse(key,0L) +  1L
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    //聚合
    other.value.foreach{
      case (key ,count)=>{
        map(key) = map.getOrElse(key,0L) + count
      }
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}

//用户访问动作表
case class UserVisitAction(date: String,//用户点击行为的日期
                           user_id: Long,//用户的ID
                           session_id: String,//Session的ID
                           page_id: Long,//某个页面的ID
                           action_time: String,//动作的时间点
                           search_keyword: String,//用户搜索的关键词
                           click_category_id: Long,//某一个商品品类的ID
                           click_product_id: Long,//某一个商品的ID
                           order_category_ids: String,//一次订单中所有品类的ID集合
                           order_product_ids: String,//一次订单中所有商品的ID集合
                           pay_category_ids: String,//一次支付中所有品类的ID集合
                           pay_product_ids: String,//一次支付中所有商品的ID集合
                           city_id: Long)//城市 id
// 输出结果表
case class CategoryCountInfo(var categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long)//支付次数
