package com.yuwenzhi.v1.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.junit.Test

class Test_key_value_operator {

  //partitionBy()
  //dddddd
  @Test
  def Test_partitionBy(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

     val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)

//    rdd1.partitionBy(new HashPartitioner(2)).mapPartitionsWithIndex(
//      (index,pItems) => {
//        pItems.map((index,_))
//    }).foreach(println)

    //将分区器设置成自定义的分区器
    rdd1.partitionBy(new MyPartitioner(10)).mapPartitionsWithIndex(
      (index,PItems)=>{
        PItems.map((index,_))
    }).foreach(println)

    //5.关闭
    sc.stop()
  }


  //reduceBy(): 对相同的key的数据将value进行聚合，在shuffle前会进行combiner(预聚合）
  //groupBy(): 根据key进行分组，直接进行shuffle,不对value 进行聚合，只会形成一个seq
  @Test
  def Test_reduceByAndgroupBy(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("a",3),("c",1),("b",3)))

    //对value进行相加操作
    rdd.reduceByKey(_+_).foreach(println)
    //reduceBykey和groupBykey都是具有宽依赖的转化算子，foreach()是行动算子
    //=> 1. 有两个job:job0 和job1 (控制台打印的日志可以看出来） 看日志！！！
    //        job->(DAGScheduler对job切分成stage[resultStage,shuffMapStage]
    //        stage->task: 数量等于stage中最后一个转换Rdd（reduceBykey())的分区数
    //              (TaskScheduler获取一个job的所有task然后序列化发往executor)
    //=> 2. job0有一个宽依赖（reduceBykey),所以有两个stage(控制台打印的日志可以看出来stage0,stage1）,job1同理
    //=> 3.
   rdd.groupByKey().foreach(println)

    //5.关闭
    sc.stop()
  }

  //aggregateByKey(初始值)(分区内逻辑，分区间逻辑)
  //foldByKey(初始值)(分区内和分区间同一个逻辑)
  @Test
  def Test_aggregateByKeyAndfoldBykey(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //需求： 统计每个分区相同key对应值的最大值，并将不同分区间的相同key的对应值进行相加
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3),("c",1),("b",3)),3)

    rdd.mapPartitionsWithIndex((index,item)=>{
      item.map((index,_))
    }).collect().foreach(println)

    //aggreateByKey(参数一)(参数二,参数三)
    //参数一： 初始值 参与参数二的执行
    //参数二： 初始值与同一个分区中同一组的多个value做某种操作
    //参数三： 多个分区的同一组的value做某种操作
    rdd.aggregateByKey(10)((zv,el)=>math.max(zv,el),_+_).collect().foreach(println)
    //5.关闭
    sc.stop()
  }

  //foldByKey(初始值)(分区内和分区间同一个逻辑)
  @Test
  def Test_foldByKey(): Unit ={
    //需求： 实现一个wordCount
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

     val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",2),("b",3),("c",4),("a",6)),2)
    rdd.foldByKey(0)(_+_).collect().foreach(println)

    //5.关闭
    sc.stop()
  }

  //combineByKey(将数据进行结构转化)(分区内的逻辑，分区间的逻辑）
  @Test
  def Test_combineByKey(): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98),("c",4))
    val rdd: RDD[(String, Int)] = sc.makeRDD(list,2)

    // 需求：计算每种key对应值的总和，然后求每组key对应值的平均值

    //每个分区的同一组key进入到combinByKey（，，，）
    //参数一： 数据结构（每个分区的同一组的value）传给参数一
    //        结构转化(预处理) 将相同组的value结构转化 例：分区一 a组 ->（88，1）（91,1）
    //参数二： 数据格式（相同分区内，相同组的value）传给参数二
    //        分区内，对 a组 中的多个二元组的中的 _1 和 _2 进行merge
    //参数三： 数据格式 (不同分区的相同组的value) 传给参数三
    //        分区间，多个分区间的 a组 的 二元组进行 merge
    //计算结果： (a,(179,2))  再 map一下即可得出结果
    //..ByKey： 不需考虑key，拿到手的都是分区内和分区间的相同key的value值，对value进行操作即可
    val rdd2: RDD[(String, (Int, Int))] = rdd.combineByKey(
      (_, 1),
      (acc: (Int, Int), nextVal) => {
        (acc._1 + nextVal, acc._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    rdd2.map(t => (t._1, t._2._1 / t._2._2.toDouble)).collect().foreach(println)

    //5.关闭
    sc.stop()
  }

  @Test
  def Test_sortByKey(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1,"aa"),(4,"jj"),(5,"ac")))
    rdd.sortByKey(false).collect().foreach(println)

    //5.关闭
    sc.stop()
  }

  @Test
  //mapValues(): 操作value,返回的仍然是（K,V)
  def Test_mapValues(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1,"aa"),(4,"jj"),(5,"ac")))

    rdd.mapValues(_+"v").collect().foreach(println)

    //5.关闭
    sc.stop()
  }

  @Test
  def Test_join(): Unit ={
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1,"a"),(4,"b"),(6,"ac")))

    val rdd2: RDD[(Int, String)] = sc.makeRDD(Array((1,"c"),(4,"d"),(5,"ac")))

    rdd.join(rdd2).collect().foreach(println)

    rdd.cogroup(rdd2).collect().foreach(println)

    //5.关闭
    sc.stop()
  }

}

/**
 *
 * @param num
 */
class MyPartitioner(num:Int) extends Partitioner{
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    if(key.isInstanceOf[Int]){
      //将key强转成Int
      val keyInt: Int = key.asInstanceOf[Int]
      if(keyInt % 3 == 0) 0 else 1
    }else{
      //非数字分到0分区
      0
    }
  }
}