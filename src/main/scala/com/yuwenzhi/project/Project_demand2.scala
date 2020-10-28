package com.yuwenzhi.project

import com.yuwenzhi.project.Project_demand2.sc
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Test}

import scala.collection.mutable.{ArrayOps, ListBuffer}

class Project_demand2 {
  @After
  def close(): Unit = {
    //5.关闭
    sc.stop()
  }

  //Top10热门品类中每个品类的Top10活跃Session统计
  @Test
  def test_demand2(): Unit = {
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
    val categoryTop10: Array[CategoryCountInfo] = resultRdd.take(10)
    val ids: Array[String] = categoryTop10.map(_.categoryId)
    //优化： 使用广播变量
    val broadcastIds: Broadcast[Array[String]] = sc.broadcast(ids)

    //使用actionRdd的处理后的结果，过滤出热门品类Top10的数据
    val top10Rdd: RDD[UserVisitAction] = actionRdd.filter(action => {
      //必须点击才是有效的数据
      if (action.click_category_id != -1) {
        broadcastIds.value.contains(action.click_category_id.toString)
      } else {
        false
      }
    })
    //结构变换  => ((category_id-session),1)
    val cateIdAndSessionToOne: RDD[(String, Int)] = top10Rdd.map(action => {
      ((action.click_category_id + "--" + action.session_id), 1)
    })
    //聚合
    val cateIdAndSessionToSum: RDD[(String, Int)] = cateIdAndSessionToOne.reduceByKey(_ + _)
    //统计的是热门品类top10下前十的session信息（点击，下单，支付等操作）
    //所以为了方便后面根据品类id分组，组内排序
    //需要结构变换 => (cateId ,(session, sum))
    val cateIdToSessionAndSum: RDD[(String, (String, Int))] = cateIdAndSessionToSum.map {
      case (k, v) => {
        val idAndSession: Array[String] = k.split("--")
        (idAndSession(0), (idAndSession(1), v))
      }
    }
    //分组，组内排序并取Top10
    val resultSessionRdd: RDD[(String, List[(String, Int)])] = cateIdToSessionAndSum.groupByKey().mapValues(sessionAndSum => {
      sessionAndSum.toList.sortWith((left, right) => {
        left._2 > right._2
      }).take(10)
    })
    resultSessionRdd.collect().foreach(println)
  }

  //Top10热门品类中每个品类的Top10城市统计
  @Test
  def test_demand2City(): Unit = {
    val rdd01: RDD[String] = sc.textFile("input/user_visit_action.txt")
//    需求分析：
//    1.先求热门品类Top10，就是统计品类下面的：点击数，下单数，支付数后，根据以上信息排名
//        统计优先级：点击数 > 下单数 > 支付数
//    1.1 将数据封装到样例类 UserVisitAction
//    1.2 结构转换，提取与需求相关的信息 封装到CategoryCountInfo对象中
//    1.3 聚合点击数，下单数，支付数 => RDD[CategoryCountInfo]
//    1.4 排序取Top10

    //2.top10下再求Top10城市，
    // 需求分析：就是求这些品类在那些城市比较火
    // 2.0 先过滤 top10的品类数据
    // 2.1 将（品类-城市）绑定在一起，统计次数
    // 2.2 统计好后，需求品类下面的城市Top10，即对品类分组，对城市排名
    // 2.3 结构转化： (（品类-城市）, sum） => （品类，（城市，sum))
    // 2.4 品类分组，组内排序取Top10

    //1.1 将数据封装到样例类 UserVisitAction
    val actionRdd: RDD[UserVisitAction] = rdd01.map {
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
    }

    println("----------------------需求2 ： Top10热门品类------------------------------")

    //1.2 结构转换，提取与需求相关的信息 封装到CategoryCountInfo对象中
    //1.2.1 根据每条记录分析：
    // 包含一次点击数(id,1,0,0)
    // 多次下单数(id,0,1,0)...
    // 多次支付数(id,0,0,1)...
    // 可映射出多个CategoryCountInfo对象的集合，然后扁平化 => RDD[CategoryCountInfo]
    val countInfoRdd: RDD[(String, CategoryCountInfo)] = actionRdd.flatMap {
      case action: UserVisitAction => {
        //有点击就统计点击数，没有再统计下单数，再没有就统计支付数
        if (action.click_category_id != -1) {
          List((action.click_category_id.toString, CategoryCountInfo(action.click_category_id.toString, 1, 0, 0)))
        } else if (action.order_category_ids != "null") {
          val orderCountInfos = new ListBuffer[(String, CategoryCountInfo)]()
          val ids: Array[String] = action.order_category_ids.split(",")
          for (elem <- ids) {
            orderCountInfos.append((elem, CategoryCountInfo(elem, 0, 1, 0)))
          }
          orderCountInfos
        } else if (action.pay_category_ids != "null") {
          val payCountInfos = new ListBuffer[(String, CategoryCountInfo)]()
          val ids: Array[String] = action.pay_category_ids.split(",")
          for (elem <- ids) {
            payCountInfos.append((elem, CategoryCountInfo(elem, 0, 0, 1)))
          }
          payCountInfos
        } else {
          Nil
        }
      }
      case _ => {
        Nil
      }
    }
    //1.3 聚合点击数，下单数，支付数 => RDD[CategoryCountInfo]
    //考虑，到此可以使用两种方法
    // 一：使用groupBy分组，组内元素，使用reduce对相同品类的CategoryCountInfo进行聚合
    // 二：由于业务逻辑是累加，可以换成reduceByKey() 分区内（预聚合） 不会影响业务逻辑 （求平均值不可用reduceByKey())
    //显然方法二 更优，但是此时的RDD类型为 RDD[CategoryCountInfo]，没有key
    //如果是 RDD[(cateId,CategoryCountInfo)]类型即可，因此需迭代上面的代码
    val countReduceInfoRdd: RDD[(String, CategoryCountInfo)] = countInfoRdd.reduceByKey {
      case (info1, info2) => {
        info1.clickCount = info1.clickCount + info2.clickCount
        info1.orderCount = info1.orderCount + info2.orderCount
        info1.payCount = info1.payCount + info2.payCount
        info1
      }
    }
    //1.4 排序取Top10
    val categoryTop10: Array[CategoryCountInfo] = countReduceInfoRdd.map(_._2).sortBy(countInfo => {
      (countInfo.clickCount, countInfo.orderCount, countInfo.payCount)
    }, false).take(10)

    categoryTop10.foreach(println)

    println("----------------------需求2 ： Top10热门品类中每个品类的Top10活跃城市统计------------------------------")

    //2.top10品类下再求Top10城市，
    // 需求分析：就是求这些品类在那些城市比较火
    // 2.0 先过滤 top10的品类数据
    // 2.0.1 使用广播变量  一种优化手段
    val categoryIds: Array[String] = categoryTop10.map(_.categoryId)
    val b_categoryIds: Broadcast[Array[String]] = sc.broadcast(categoryIds)
    // 2.0.2 过滤
    val cateTop10Rdd: RDD[UserVisitAction] = actionRdd.filter(
      action => b_categoryIds.value.contains(action.click_category_id.toString))
    // 2.1 将（品类id-城市）绑定在一起，统计次数
    // 2.1.1 结构变换
    val cateAndCity_singleRdd: RDD[(String, Int)] = cateTop10Rdd.map {
      action => {
        ((action.click_category_id + "-" + action.city_id), 1)
      }
    }
    // 2.1.2 统计总数
    val cateAndCity_sumRdd: RDD[(String, Int)] = cateAndCity_singleRdd.reduceByKey(_ + _)
    // 统计好后，求品类下面的城市Top10，即对品类分组，对城市排名
    // 2.3 结构转化： (（品类-城市）, sum） => （品类，（城市，sum))
    val cateWithCityAndSum: RDD[(String, (String, Int))] = cateAndCity_sumRdd.map {
      case (k, sum) => {
        val cateIdAndCity: Array[String] = k.split("-")
        (cateIdAndCity(0), (cateIdAndCity(1), sum))
      }
    }
    // 2.4 品类分组，组内排序取Top10
    val cateUnderCityTop10: RDD[(String, List[(String, Int)])] = cateWithCityAndSum.groupByKey().mapValues(datas => {
      datas.toList.sortWith((left, right) => {
        left._2 > right._2
      }).take(10)
    })
    cateUnderCityTop10.collect().foreach(println)

    println("----------------------需求3 ： 页面单跳转化率------------------------------")

    // 需求分析： 页面单跳转化率
    // 单跳： page1 - page2 、 page2 - page3 、......
    // 转化率 = (page1-page2)次数 / page1 的总次数

    // 1.  计算分母，相当于wordCount
    // 1.1 要统计的页面（1,2,3,4,5,6,7） 过滤数据
    val pageIds = List(1L, 2L, 3L, 4L, 5L, 6L, 7L)
    val b_pageIds: Broadcast[List[Long]] = sc.broadcast(pageIds.init) //分母不需统计page7，最后的运算式为：6-7 / 7
    val actionPageInfoRdd: RDD[UserVisitAction] =
      actionRdd.filter(action => b_pageIds.value.contains(action.page_id))
    //将分母数据整理成 pageId -> sum 的形式
    val pageIdAndSumMap: collection.Map[Long, Long] = actionPageInfoRdd.map(
      action =>
        (action.page_id, 1L))
      .reduceByKey(_ + _).collectAsMap()

    //2.  计算分子
    //2.0 根据用户信息(session_id)分组 ： 因为一个session_id对应多个用户动作
    //2.1 组内排序（由于采集的数据可能来自不同的服务器，用户动作顺序会错乱，但是有动作时间
    //    所以只需按照action_time排序， 即可得到page1、page2、page3、page4...的正确顺序)
    val pageAndPageSingleRdd: RDD[((Long, Long), Long)] = actionRdd.groupBy(_.session_id).mapValues {
      datas => {
        val sortList: List[UserVisitAction] = datas.toList.sortWith((left, right) => {
          //自然时间排序
          left.action_time < left.action_time
        })
        //2.2 组内提取关键信息，需形成 （page1-page2）的形式。
        val pageIdList: List[Long] = sortList.map(_.page_id)
        //拉链
        val pageToPage: List[(Long, Long)] = pageIdList.zip(pageIdList.tail)
        val demandPageIds: List[(Long, Long)] = pageIds.zip(pageIds.tail)
        //过滤出 1-2 2-3 3-4 4-5 5-6
        //结构转换 => ((page1-page2),1)的形式。
        pageToPage.filter(demandPageIds.contains(_)).map {
          case (id1, id2) => {
            ((id1, id2), 1L)
          }
        }
      }
    }.flatMap(_._2)     //2.4  去除session_id, 打散成只有(pageN-PageN+1)的数据形式后聚合，统计(pageN-PageN+1)次数
    val pageAndPageSumRdd: RDD[((Long, Long), Long)] = pageAndPageSingleRdd.reduceByKey(_+_)
    pageAndPageSumRdd.foreach{
      case (pageAndPage,sum) =>{
        val pageId: Long = pageAndPage._1.toLong
        println(pageAndPage + ":" + sum.toDouble / pageIdAndSumMap.getOrElse(pageId,1L))
      }
    }


  }

  //页面单跳转化率统计
  @Test
  def test_percentConver(): Unit = {
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
    // 要统计的页面（1,2,3,4,5,6,7）
    val ids = List(1, 2, 3, 4, 5, 6, 7)

    //要分析的单跳
    val pairPageIdList: List[String] = ids.zip(ids.tail).map {
      case (id1, id2) => {
        id1 + "-" + id2
      }
    }
    //优化：广播变量
    val broadcast: Broadcast[List[Int]] = sc.broadcast(ids)
    //过滤要统计的页面
    val filterRdd: RDD[UserVisitAction] = actionRdd.filter(action => {
      broadcast.value.init.contains(action.page_id)
    })
    //转化率 = (页面1-页面2)次数 / 页面1 的总次数
    //求分母，提取所有的页面id后统计
    val pageSumMap: collection.Map[Long, Long] = filterRdd.map(action => (action.page_id, 1L))
      .reduceByKey(_ + _).collectAsMap()

    //求分子 ： 步骤
    //0. 根据用户信息(session_id)分组
    //1. 组内排序（按照action_time排序，会形成page1、page2、page3、page4...的顺序)
    //2. 组内提取关键信息进行拉链 ,需形成 （下单-详情）的形式。
    //3. 组内，优化： 根据要分析的单跳进行过滤。再结构转化为（(pageN-PageN+1),1)的形式
    //4. 去除无用数据，打散成只有(pageN-PageN+1)的数据形式后聚合，统计(pageN-PageN+1)次数
    val pageAndpageRdd: RDD[(String, List[(String, Int)])] = actionRdd.groupBy(_.session_id).mapValues {
      datas => {
        val soreList: List[UserVisitAction] = datas.toList.sortWith {
          (left, right) => {
            //从小到大，自然排序
            left.action_time < right.action_time
          }
        }
        // 组内提取关键信息
        val pageIds: List[Long] = soreList.map(_.page_id)
        //
        val pageAndpage: List[String] = pageIds.zip(pageIds.tail).map {
          case (id1, id2) => {
            ((id1 + "-" + id2))
          }
        }
        //过滤出要分析的单跳
        //1-2 2-3...
        pageAndpage.filter(pairPageIdList.contains(_)).map((_, 1))
      }
    }
    val reducePageRdd: RDD[(String, Int)] = pageAndpageRdd.flatMap(_._2).reduceByKey(_ + _)
    //遍历分子(页面1-页面2)次数，分别计算转化率
    // 转化率 = (页面1-页面2)次数 / 页面1 的总次数
    reducePageRdd.foreach {
      case (pageAndpage, sum) => {
        val pageKey: Long = pageAndpage.split("-")(0).toLong
        println(pageAndpage + ":" + sum.toDouble / pageSumMap.getOrElse(pageKey, 1L))
      }
    }
  }

  @Test
  def test_percentConversion(): Unit = {


  }

}

object Project_demand2 {
  //1.创建SparkConf并设置App名称
  val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

  //2.创建SparkContext，该对象是提交Spark App的入口
  val sc: SparkContext = new SparkContext(conf)

}
