package sparkSql

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.rdd._

case class Student(id:Int,name:String,sex:String,age:Int,department:String)

object SqlContextDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName(SqlContextDemo.getClass.getSimpleName)
    conf.setMaster("local")
    val context = new SparkContext(conf)
    val sqlContext = new SQLContext(context)

    //sqlContext创建DataFrame的第一种方式
    //sqlContext.read.各种格式 貌似只有json有描述信息 但是都会返回一个DataFrame
    //DataFrame可以使用select("字段名")这种形式来查看某些列
    val path: String = SqlContextDemo.getClass.getResource("/").getPath.toString + "people.json"
    val jsonFrame: DataFrame = sqlContext.read.json(path)
    jsonFrame.show()
    jsonFrame.printSchema()

    jsonFrame.select("age").show
    jsonFrame.select(jsonFrame("age") + 1).show()
    jsonFrame.groupBy("age").count().show()

    jsonFrame.createTempView("jsonView")
    sqlContext.sql("select age, name from jsonView where age = 30").show()

    //sqlContext创建DataFrame的第二种方式
    //先使用SparkContext创建rdd
    //然后使用样例类 将rdd包装成rdd[样例类] 这样就注入了描述信息
    //最后使用sqlContext.createDataFrame(rdd)创建DataFrame */
    val path1: String = SqlContextDemo.getClass.getResource("/").getPath.toString + "student.txt"
    val stuRdd: RDD[String] = context.textFile(path1)
    val rowRdd: RDD[Student] = stuRdd.map(_.split(",")).map(x => Student(x(0).toInt, x(1), x(2), x(3).toInt, x(4)))
    val stuDF: DataFrame = sqlContext.createDataFrame(rowRdd)
    //stuDF.show()

//    import sqlContext.implicits._
//    rowRdd.toDF()
    stuDF.select("name").show()

    context.stop()
  }
}
