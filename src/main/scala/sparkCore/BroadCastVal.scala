package sparkCore

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 广播变量的意义
 * value定义在driver中 而使用的时候在map方法中 map方法是在executor中执行
 * 一个executor中会有多个task 所以在执行的过程中 会在每个task中都保存一份value变量 如果value很大 造成不必要的消耗
 * 将变量广播出去 在每个executor中只保留一份 所有的task都来访问这个变量 节省资源
 *
 * 核心概念
 * executor 每个worker上会有很多个executor 每个executor就是一个jvm进程 每个executor中都会初始化一个线程池等待接收任务
 * job 一个application由action算子触发的一个子任务
 * DAGScheduler action算子将application划分为job 提交给DAGScheduler DAGScheduler从后往前遍历job 根据宽依赖将job划分为stage 提交stage给TaskScheduler
 * TaskScheduler TaskScheduler根据stage中最后一个rdd的分区数来确定task任务的个数 将所有task交给clusterManager分配给executor执行
 * task 是executor线程池中的一个执行任务的线程
 */

object BroadCastVal {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("BroadCastVal")
    val context: SparkContext = new SparkContext(conf)

    val value = 100
    val broadValue: Broadcast[Int] = context.broadcast(value)

    context.makeRDD(1 to 20).map(x => x + broadValue.value).foreach(println)

    context.stop()
  }
}
