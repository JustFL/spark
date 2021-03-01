package sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

/**
 * 选中想被try/catch包围的语句 同时按下ctrl+alt+t
 * 补全等号左边语句 Ctrl+Alt+V+回车
 * 查看接口的实现类：Ctrl+Alt+B
 * 查看类继承关系图：Ctrl+Alt+U
 * 查看当前类的继承树：Ctrl+H
 * 查看一个类中有什么方法：Alt+7
 *
 */

/**
 * DataFrame = RDD[Row] + schema
 * DataFrame = Dataset[Row]
 */
case class Stu(id:Int,name:String,sex:String,age:Int,department:String)

object HelloSparkSession {
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName(HelloSparkSession.getClass.getSimpleName).master("local").getOrCreate();
    val path: String = HelloSparkSession.getClass.getResource("/").getPath.toString + "student.txt"

    //使用session创建DataFrame
    //1 这种方法无法指定字段类型 seq只能指定字段的名称 字段类型默认都是String
    val stuDF: DataFrame = session.read.csv(path)
    stuDF.printSchema()

    val schemas= Seq("id","name","sex","age","department")
    val df1: DataFrame = stuDF.toDF(schemas: _*)
    df1.printSchema()

    //2   def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame
    val rdd: RDD[String] = session.sparkContext.textFile(path)
    val rddStu: RDD[Stu] = rdd.map(_.split(",")).map(x => Stu(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    val df2: DataFrame = session.createDataFrame(rddStu)
    df2.printSchema()

    /*3
    * 使用StructType作为描述信息
    * */
    val rddString: RDD[String] = session.sparkContext.textFile(path)
    val rddRow: RDD[Row] = rddString.map(_.split(",")).map(x => Row(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))

    val structType = new StructType(
      Array(StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("sex", StringType, true),
        StructField("age", IntegerType, true),
        StructField("department", StringType, true)
      )
    )

    /*type DataFrame = Dataset[Row]*/
    val studentDF: DataFrame = session.createDataFrame(rddRow,structType)
    studentDF.show()
    studentDF.filter(x => x(3) == 20).show()
    studentDF.select("id","name").show(3)
    studentDF.groupBy("sex").count().show()

    println("---------Dataset---------")
    /*1
    * 创建DataSet*/
    import session.implicits._

    val line: Dataset[String] = session.read.textFile(path)
    val dataStu: Dataset[Stu] = line.map(x => {
      val words: Array[String] = x.split(",")
      Stu(words(0).toInt, words(1), words(2), words(3).toInt, words(4))
    })
    //有类型检查
    dataStu.filter(stu => stu.age > 20).show()

    /*补充global表的内容 使用global后 不论哪个session都可以访问 但是需要指定默认存储的库global_temp*/
    dataStu.createGlobalTempView("stu")
    session.sql("select count(*) from global_temp.stu").show()

    session.newSession().sql("select count(*) from global_temp.stu").show

    session.close()
  }
}

