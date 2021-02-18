package userFunction

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 选中想被try/catch包围的语句 同时按下ctrl+alt+t
 * 补全等号左边语句 Ctrl+Alt+V+回车
 * 查看接口的实现类：Ctrl+Alt+B
 * 查看类继承关系图：Ctrl+Alt+U
 * 查看当前类的继承树：Ctrl+H
 * 查看一个类中有什么方法：Alt+7
 *
 */

object SparkUserFunction {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","hadoop")
    val path: String = SparkUserFunction.getClass.getResource("/").getPath.toString + "student.txt"

    val session: SparkSession = new SparkSession.Builder().appName("SparkUserFunction").master("local").getOrCreate()
    val rdd: RDD[String] = session.sparkContext.textFile(path)
    val rddRow: RDD[Row] = rdd.map(_.split(",")).map(x => Row(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))

    val struct = StructType(
            StructField("id", IntegerType, true) ::
            StructField("name", StringType, true) ::
            StructField("sex", StringType, true) ::
            StructField("age", IntegerType, true) ::
            StructField("department", StringType, true) :: Nil)

    /*type DataFrame = Dataset[Row]*/
    val frame: DataFrame = session.createDataFrame(rddRow, struct)
    frame.createTempView("stuView")
    session.udf.register("len", ((x:String) => x.length))
    session.sql("select id, len(name), name from stuView").show()

    val df1: DataFrame = session.sql("select id, len(name), name from stuView")
    val rdd1: RDD[Any] = df1.rdd.map(_.getInt(1))
    rdd1.foreach(println)

    session.stop()
  }
}
