package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*模拟使用sparkstreaming读取网络数据和本地数据 注意这里的wordcount逻辑是有问题的 每次只能读取一个rdd的数据
* 以前的rdd的数据并不会进行累加 */

object SocketStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SocketStreaming").setMaster("local[4]")
    val context = new StreamingContext(conf,Seconds(3))
    /*sparkstreaming监听网络端口 使用nc -lk port命令发送字符串*/
    //val line: ReceiverInputDStream[String] = context.socketTextStream("192.168.199.10",9999)
    /*sparkstreaming监听本地文件系统 程序先运行 然后拷贝文件到监视的目录下 这里有一点非常重要 因为要模拟流式处理
    * 所以这个文件必须要在程序启动后修改一次 添加内容 或者修改文件名字 这样程序才能识别（文件的修改日期一定要晚于程序启动时间）*/
    val line: DStream[String] = context.textFileStream("C:\\movie")
    val result: DStream[(String, Int)] = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    result.print()

    context.start()
    context.awaitTermination()
  }
}
