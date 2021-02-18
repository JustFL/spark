package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 选中想被try/catch包围的语句 同时按下ctrl+alt+t
 * 补全等号左边语句 Ctrl+Alt+V+回车
 * 查看接口的实现类：Ctrl+Alt+B
 * 查看类继承关系图：Ctrl+Alt+U
 * 查看当前类的继承树：Ctrl+H
 * 查看一个类中有什么方法：Alt+7
 *
 */

object HelloWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local").setAppName("HelloSpark")
    val context: SparkContext = new SparkContext(conf)

    val classpath: String = HelloWordCount.getClass.getResource("/").getPath.toString

    val line: RDD[String] = context.textFile(classpath+"/words.txt")
    line.flatMap(_.split(" ")).map(x => (x,1)).reduceByKey(_+_).foreach(x=>println(x))

    context.stop()
  }
}
