package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object MovieProblem_9 {

  val checkType : (String) => (Set[String]) = (s : String) => {
    s.split("\\|").toSet
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName(MovieProblem_7.getClass.getSimpleName)
    conf.setMaster("local")
    val context = new SparkContext(conf)
    context.setLogLevel("WARN")

    val movie: RDD[String] = context.textFile("C:\\movie\\movies.dat")
    val rating: RDD[String] = context.textFile("C:\\movie\\ratings.dat")
    val user: RDD[String] = context.textFile("C:\\movie\\users.dat")

    //数据格式为：  2::M::56::16::70072
    val user_content: RDD[Array[String]] = user.map(_.split("::"))
    //数据格式为： 2::Jumanji (1995)::Adventure|Children's|Fantasy
    val movie_content: RDD[Array[String]] = movie.map(x => x.split("::"))
    //数据格式为：  1::1193::5::978300760
    val rate_content: RDD[Array[String]] = rating.map(x => x.split("::"))

    //8
    val p8_rdd1: RDD[Array[String]] = movie_content.filter(x => MovieProblem_7.checkYear(x(1)) == 1997 && checkType(x(2)).contains("Comedy"))
    val p8_rdd2: RDD[(String, String)] = p8_rdd1.map(x => (x(0), x(1)))
    val p8_rdd3: RDD[(String, String)] = rate_content.map(x => (x(1), x(2)))
    val p8_rdd4: RDD[(Int, Double)] = MovieProblem_7.avg(p8_rdd2.join(p8_rdd3).map(x => (x._1.toInt, x._2._2.toInt)).groupByKey())
    //p8_rdd4.sortBy(_._2,false).take(10).foreach(println)

    //9
    val p9_rdd1: RDD[(String, (String, String))] = movie_content.map(x => (x(0), (x(1), x(2))))
    val p9_rdd2: RDD[(String, Int, String)] = p9_rdd1.join(p8_rdd3).map(x => (x._2._1._1, x._2._2.toInt, x._2._1._2))
    //(Hook (1991),3,Adventure)
    val p9_rdd3: RDD[(String, Int, String)] = p9_rdd2.flatMap(x => {
      val tuples: ArrayBuffer[(String, Int, String)] = ArrayBuffer[(String, Int, String)]()
      val strings: Array[String] = x._3.toString.split("\\|")
      for (s <- strings) {
        val tuple: (String, Int, String) = (x._1, x._2, s)
        tuples += tuple
      }
      tuples
    })
    //(High Art (1998),3.0)
    val p9_rdd4: RDD[(String, Double)] = p9_rdd3.groupBy(_._1).map(x => {
      var count = 0
      var sum = 0
      val iter: Iterator[(String, Int, String)] = x._2.toIterator
      while (iter.hasNext) {
        count += 1
        sum += iter.next()._2.toInt
      }
      (x._1, (sum / count).toDouble)
    })


    val p9_rdd5: RDD[(String, (String, Double))] = p9_rdd3.map(x => (x._1, x._3)).join(p9_rdd4)
    val value: RDD[(String, (String, Double))] = p9_rdd5.groupBy(_._2._1).flatMap(x => {
      val iter: Iterator[(String, (String, Double))] = x._2.toIterator
      val array: Array[(String, (String, Double))] = iter.toArray
      array.sortBy(_._2._2).takeRight(5)
    })
    value.foreach(println)



    context.stop()
  }
}
