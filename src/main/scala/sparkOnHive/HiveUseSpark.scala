package sparkOnHive

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 选中想被try/catch包围的语句 同时按下ctrl+alt+t
 * 补全等号左边语句 Ctrl+Alt+V+回车
 * 查看接口的实现类：Ctrl+Alt+B
 * 查看类继承关系图：Ctrl+Alt+U
 * 查看当前类的继承树：Ctrl+H
 * 查看一个类中有什么方法：Alt+7
 *
 */
object HiveUseSpark {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","hadoop")

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("HiveUseSpark")
    val context = new SparkContext(conf)

    val hiveContext = new HiveContext(context)
    hiveContext.sql("select * from mydb.tt").show()

    context.stop()
  }
}
