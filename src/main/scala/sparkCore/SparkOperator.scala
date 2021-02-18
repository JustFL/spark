package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
 * 选中想被try/catch包围的语句 同时按下ctrl+alt+t
 * 补全等号左边语句 Ctrl+Alt+V+回车
 *
 * 算子分为action和transform两种
 * transform 是一个rdd向另一个rdd转化
 * action    是一个rdd向scala集合转化
 */
object SparkOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("OperatorTest")
    conf.setMaster("local")
    val context = new SparkContext(conf)

    val l1 = List(1,2,3,4,5,5,6,6,7)
    val l2 = List(3,4,5)
    val arr1 = Array("hello huangbo","hello xuzheng","hello wangbaoqiang")
    val kv1 = Array(("a",1),("a",1),("a",2),("c",3),("b",2))
    val kv2 = Array(("d",1),("d",1),("d",2),("f",3),("e",2),("a",2))

    val rdd1: RDD[Int] = context.parallelize(l1,3)
    val rdd2 = context.makeRDD(arr1)
    val rdd3 = context.makeRDD(l2)
    val rdd4 = context.makeRDD(kv1)
    val rdd5 = context.makeRDD(kv2)

    rdd1.map(_+1).foreach(println)
    println("---------")
    rdd1.filter(_%2==1).foreach(println)
    println("---------")
    println(rdd1.reduce((x, y) => x + y))
    println("---------")
    rdd1.distinct.foreach(println)
    println("---------")
    println(rdd1.first())
    println("---------")
    println(rdd1.take(3).mkString(","))
    println("---------")
    rdd2.flatMap(_.split(" ")).foreach(println)
    println("---------")

    rdd1.mapPartitions(iter => {
      val tuples = new ArrayBuffer[(Int,Int)]()
      var count = 0
      var sum = 0
      while(iter.hasNext){
        count += 1
        sum += iter.next()
      }
      tuples+=((count,sum))
      tuples.iterator
    }).foreach(x => println(x._1,x._2))
    println("---------")

    rdd2.mapPartitions(x => {
      val arrTmp = new ArrayBuffer[String]()
      while(x.hasNext){
        arrTmp += x.next() + " Partition"
      }
      arrTmp.iterator
    }).foreach(println)
    println("---------")

    rdd1.mapPartitionsWithIndex((index, iter) => {
      var count = 0
      val tuples = new ArrayBuffer[(Int,Int)]()
      while (iter.hasNext){
        count += iter.next()
      }
      tuples += ((index,count))
      tuples.iterator
    }).foreach(x => println(x._1,x._2))
    println("---------")

    //1.是否放回 2.取rdd的比例
    rdd1.sample(false,0.5,6).foreach(println)

    println("----union-----")
    rdd1.union(rdd3).foreach(println)

    println("----intersection-----")
    rdd1.intersection(rdd3).foreach(println)

    println("----distinct-----")
    rdd1.distinct.foreach(println)

    println("----groupByKey-----")
    rdd4.groupByKey().foreach(x => println(x._1,x._2.toBuffer))

    println("-----sortByKey----")
    rdd4.sortByKey().foreach(x => println(x._1,x._2))

    println("----sortBy-----")
    rdd4.sortBy(x => x._2,false).foreach(x => println(x._1,x._2))

    println("----swap-----")
    rdd4.map(x => x.swap).sortByKey().map(x => x.swap).foreach(x => println(x._1,x._2))

    println("----reduceByKey-----")
    rdd4.reduceByKey(_+_).foreach(x => println(x._1,x._2))

    println("----aggregateByKey-----")
    //(_+_, _+_) 一个代表combiner 一个reducer
    rdd4.aggregateByKey(0)(_+_, _+_).foreach(x => println(x._1,x._2))

    println("----join-----")
    rdd4.join(rdd5).foreach(x => println(x._1,x._2._1,x._2._2));

    println("----cogroup-----")
    rdd4.cogroup(rdd5).sortByKey().foreach(x => println(x._1,x._2._1.mkString("-"),x._2._2.mkString("-")))

    println("----coalesce-----")
    rdd5.coalesce(3)

    println("----subtract-----")
    rdd1.subtract(rdd3).foreach(println)

    println("----subtractByKey-----")
    rdd4.subtractByKey(rdd5).foreach(println)

    println("----reduceByKeyLocally-----")
    val stringToInt: collection.Map[String, Int] = rdd5.reduceByKeyLocally(_+_)
    stringToInt.foreach(x => println(x._1,x._2))

    println("----top-----")
    rdd1.top(3).foreach(println)

    println("----takeOrdered-----")
    rdd1.takeOrdered(3).foreach(println)

    println("----countByKey-----")
    rdd5.countByKey().foreach(x => println(x._1,x._2))

    println("----countByValue-----")
    val tupleToLong: collection.Map[(String, Int), Long] = rdd5.countByValue()
    tupleToLong.foreach(x => println(x._1,x._2))

    println("----mapValues-----")
    rdd5.mapValues(x => "hello" + x).foreach(println)



    context.stop
  }
}
