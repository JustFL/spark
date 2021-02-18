package userFunction

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

/**
 * 选中想被try/catch包围的语句 同时按下ctrl+alt+t
 * 补全等号左边语句 Ctrl+Alt+V+回车
 * 查看接口的实现类：Ctrl+Alt+B
 * 查看类继承关系图：Ctrl+Alt+U
 * 查看当前类的继承树：Ctrl+H
 * 查看一个类中有什么方法：Alt+7
 *
 */

case class Student(id:Int,name:String,sex:String,age:Int,department:String)
object SparkAvgAge{
  def main(args: Array[String]): Unit = {

    val path: String = SparkUserFunction.getClass.getResource("/").getPath.toString + "student.txt"
    val session: SparkSession = SparkSession.builder().appName(SparkAvgAge.getClass.getSimpleName).master("local").getOrCreate()
    val rddStu: RDD[Student] = session.sparkContext.textFile(path)
      .map(_.split(","))
      .map(x => Student(x(0).toInt, x(1), x(2), x(3).toInt, x(4)))
    val df1: DataFrame = session.createDataFrame(rddStu)
    df1.createTempView("stu")
    session.udf.register("mymax",AvgAge)
    session.sql("select department,mymax(age) from stu group by department").show()
  }
}

object AvgAge extends UserDefinedAggregateFunction{
  //指定输入的数据类型
  override def inputSchema: StructType = StructType(
    StructField("age",IntegerType,true)::Nil)

  //指定辅助数据的类型
  override def bufferSchema: StructType = StructType(
    StructField("maxage",IntegerType,true)::Nil)

  //指定最终输出的数据类型
  override def dataType: DataType = IntegerType

  //初始化辅助变量
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,0)
  }

  //计算逻辑 buffer表示辅助变量 input表示每次的真实数据 这个方法作用在局部
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val currentMax: Int = buffer.getInt(0)
    val newMax: Int = input.getInt(0)
    val trueMax: Int = if (currentMax > newMax) currentMax else newMax
    buffer.update(0,trueMax)
  }

  //这个方法作用在全局 表示对局部数据进行整体逻辑运算
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val currentMax: Int = buffer1.getInt(0)
    val newMax: Int = buffer2.getInt(0)
    val trueMax: Int = if (currentMax > newMax) currentMax else newMax
    buffer1.update(0,trueMax)
  }

  //最终的结果
  override def evaluate(buffer: Row): Any = buffer.getInt(0)

  //如果输入和输出的类型一直 就为true
  override def deterministic: Boolean = true
}
