package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*实现rdd之间的累加 并且实现steaming的高可用 例如程序崩溃后 再次启动能继续上次的进度*/
object AdvanceStreaming {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","hadoop")
    //因为要累加rdd 需要一个中间值的存放目录 并且这个目录可以存放context对象 实现高可用
    val dir = "hdfs:///tempStream"

    def createNewStreamingContext:StreamingContext = {
      val conf: SparkConf = new SparkConf().setAppName("AdvanceStreaming").setMaster("local[4]")
      val context = new StreamingContext(conf, Seconds(3))

      //指定存放目录
      context.checkpoint(dir)

      //val lineStream: DStream[String] = context.textFileStream("C:\\movie")
      //(updateFunc : scala.Function2[scala.Seq[V], scala.Option[S], scala.Option[S]])
      val lineStream: ReceiverInputDStream[String] = context.socketTextStream("192.168.199.10",9999)
      val upFunc = (values:Seq[Int], buffer:Option[Int]) => {
        val i: Int = values.sum + buffer.getOrElse(0)
        Some(i)
      }
      //由updateStateByKey方法实现rdd之间的累加
      val wcStream: DStream[(String, Int)] = lineStream.flatMap(_.split(" ")).map((_,1)).updateStateByKey(upFunc)
      wcStream.print()

      context
    }

    //重新启动后 到存放目录去查找 有原来的StreamingContext就恢复 否则创建新的StreamingContext对象
    val newContext: StreamingContext = StreamingContext.getActiveOrCreate(dir,createNewStreamingContext _)

    newContext.start()
    newContext.awaitTermination()
  }
}
