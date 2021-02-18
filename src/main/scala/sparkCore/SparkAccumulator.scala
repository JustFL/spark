package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.LongAccumulator

object SparkAccumulator {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("SparkAccumulator")
    val context: SparkContext = new SparkContext(conf)

    //在driver中定义累加器
    val accumulator: LongAccumulator = context.longAccumulator("count")

    context.makeRDD(1 to 20).map(x => {
      //在executor中使用 累加器默认经过序列化 可以直接传输
      accumulator.add(1)
      x + 1
    }).foreach(println)

    println("accumulator: " + accumulator.value)
    context.stop()
  }
}