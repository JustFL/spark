package sparkStreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BlackListStreaming {

  def updateFunc(values:Seq[Int], buffer:Option[Int]) : Option[Int] = {
    val i: Int = values.sum + buffer.getOrElse(0)
    Some(i)
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","hadoop")

    val conf: SparkConf = new SparkConf().setAppName("BlackListStreaming").setMaster("local[4]")
    val sparkContext = new SparkContext(conf)
    val context = new StreamingContext(sparkContext, Seconds(4))
    context.checkpoint("hdfs:///blackStream")

    val black = List("#","$","%")
    val broadValue: Broadcast[List[String]] = context.sparkContext.broadcast(black)

    val lineStream: ReceiverInputDStream[String] = context.socketTextStream("hadoop",9999)
    val wordsStream: DStream[String] = lineStream.flatMap(_.split(" "))
    val resStream: DStream[String] = wordsStream.transform(x => {
      val blackList: List[String] = broadValue.value
      val resRdd: RDD[String] = x.filter(x => !blackList.contains(x))
      resRdd
      }
    )

    resStream.map((_, 1)).updateStateByKey(updateFunc _).print()

    context.start()
    context.awaitTermination()
  }
}
