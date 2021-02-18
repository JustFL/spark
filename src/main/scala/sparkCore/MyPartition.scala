package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * 选中想被try/catch包围的语句 同时按下ctrl+alt+t
 * 补全等号左边语句 Ctrl+Alt+V+回车
 * 查看接口的实现类：Ctrl+Alt+B
 * 查看类继承关系图：Ctrl+Alt+U
 * 查看当前类的继承树：Ctrl+H
 * 查看一个类中有什么方法：Alt+7
 *
 */
//Partitioner
class MyPartition(val nums:Int) extends Partitioner{
  override def numPartitions: Int = nums

  override def getPartition(key: Any): Int = {
    key.toString.toInt % nums
  }
}

object MyPartitionTest{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MyPartitionTest").setMaster("local")
    val context: SparkContext = new SparkContext(conf)

    val ints: RDD[Int] = context.makeRDD(1 to 20, 3)
    val result: RDD[(Int, Int)] = ints.map(x => (x, 1)).partitionBy(new MyPartition(4))
    result.foreachPartition(iter => {
      while (iter.hasNext){
        val tuple: (Int, Int) = iter.next()
        println(tuple)
      }
      println("-------")
    })

    context.stop()
  }
}
