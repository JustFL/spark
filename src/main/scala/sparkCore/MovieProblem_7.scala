package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

object MovieProblem_7 {

  val checkYear : (String) => (Int) = (s:String) => {
    val regex = new Regex("(\\d+)")
    regex.findFirstIn(s).get.toInt
  }

  val avg: RDD[(Int, Iterable[Int])] => RDD[(Int, Double)] = (data:RDD[(Int, Iterable[Int])]) => {
    data.map(x => {
      var count = 0
      var sum = 0
      val iter: Iterator[Int] = x._2.toIterator
      while (iter.hasNext) {
        count += 1
        sum += iter.next()
      }
      (x._1, (sum / count).toDouble)
    })
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

    //1
    val p1_rdd1 = movie_content.map(x => (x(0).toInt, x(1)))
    val result1: Array[(String, Int)] = rate_content
      .map(x => (x(1).toInt, 1))
      .reduceByKey(_ + _)
      .join(p1_rdd1)
      .sortBy(x => x._2._1, false)
      .take(10)
      .map(x => (x._2._2, x._2._1))

    //result1.foreach(println)

    //2
    val p2_rdd1 = user_content.filter(_ (1) == "M").map(x => (x(0).toInt, x(1)))
    val p2_rdd2: RDD[(Int, ((Int, Int), String))] = rate_content.map(x => (x(0).toInt, (x(1).toInt, x(2).toInt))).join(p2_rdd1)
    val p2_rdd3: RDD[(Int, Int)] = p2_rdd2.map(x => (x._2._1))
    val p2_rdd4: RDD[(Int, Double)] = avg(p2_rdd3.groupByKey())
//    p2_rdd4.sortBy(_._2, false).join(p1_rdd1)
//      .map(x => ("M",x._2._2,x._2._1))
//      .sortBy(_._3,false)
//      .take(10)
//      .foreach(println)

    //3
    val p3_rdd1: RDD[(Int, String)] = user_content.filter(_ (1) == "F").map(x => (x(0).toInt, x(1)))
    val p3_rdd2: RDD[(Int, (Int, Int))] = rate_content.map(x => (x(0).toInt, (x(1).toInt, 1)))

    val p3_rdd3: RDD[((String, Int), Int)] = p3_rdd1.join(p3_rdd2).map(x => ((x._2._1, x._2._2._1), x._2._2._2))
    val p3_rdd4: RDD[((String, Int), Int)] = p3_rdd3.reduceByKey(_ + _)
    val p3_rdd5: RDD[(Int, (String, Int))] = p3_rdd4.map(x => (x._1._2, (x._1._1, x._2)))
    val p3_rdd6: RDD[(Int, ((String, Int), String))] = p3_rdd5.join(p1_rdd1)
    //p3_rdd6.map(x => (x._2._1._1,x._2._2,x._2._1._2)).sortBy(x => x._3,false).take(10).foreach(println)


    //4
    val p4_rdd1: RDD[(Int, Int)] = user_content.filter(x => x(1) == "M" && (x(2).toInt >= 18 && x(2).toInt <= 24)).map(x => (x(0).toInt,1))
    val p4_rdd2: RDD[(Int, (Int, Int))] = rate_content.map(x => (x(0).toInt, (x(1).toInt, x(2).toInt)))
    val p4_rdd3: RDD[(Int, Int)] = p4_rdd1.join(p4_rdd2).map(_._2._2)
    val p4_rdd4: RDD[(Int, Double)] = avg(p4_rdd3.groupByKey())

    val p4_rdd5: RDD[(String, Double)] = p1_rdd1.join(p4_rdd4).map(x => x._2)
    //p4_rdd5.sortBy(_._2,false).take(10).foreach(println)

    //5
    val p5_rdd1: RDD[(String, (String, String))] = rate_content.filter(x => x(1) == "2116").map(x => (x(0), (x(1), x(2))))
    val p5_rdd2: RDD[(String, String)] = user_content.map(x => (x(0), x(2)))
    val p5_rdd3: RDD[(String, ((String, String), String))] = p5_rdd1.join(p5_rdd2)
    val p5_rdd4: RDD[(Int, Int)] = p5_rdd3.map(x => (x._2._2.toInt, x._2._1._2.toInt))
    val p5_rdd5: RDD[(Int, Double)] = avg(p5_rdd4.groupByKey())
    //p5_rdd5.foreach(println)

    //6
    val p6_rdd1: RDD[(Int, (String, (Int, Int)))] = p3_rdd1.join(p4_rdd2)
    val p6_value1: Int = p6_rdd1.map(x => (x._1, 1)).reduceByKey(_ + _).sortBy(_._2, false).map(_._1).first()
    val p6_value2: Array[Int] = p6_rdd1.filter(x => x._1 == p6_value1).map(x => (x._1, x._2._2)).sortBy(_._2._2, false).map(_._2._1).take(10)
    val p6_value3: Set[Int] = p6_value2.toSet
    val p6_rdd2: RDD[(Int, Int)] = rate_content.map(x => (x(1).toInt, x(2).toInt)).filter(x => p6_value3.contains(x._1))
    val p6_rdd3: RDD[(Int, Double)] = avg(p6_rdd2.groupByKey())
    //p6_rdd3.join(p1_rdd1).map(x => (x._2._2,x._2._1)).foreach(println)

    //7
    val p7_rdd1: RDD[(Int, Int)] = rate_content.map(x => (x(1).toInt, x(2).toInt))
    val p7_rdd2: RDD[(Int, Double)] = avg(p7_rdd1.groupByKey())
    val p7_rdd3: RDD[(Int, (Double, String))] = p7_rdd2.filter(_._2 >= 4.0).join(p1_rdd1)
    val p7_value1: (Int, Int) = p7_rdd3.map(x => {
      (checkYear(x._2._2.toString), 1)
    }).reduceByKey(_ + _).sortBy(_._2, false).first()

    val p7_rdd4: RDD[(Int, String)] = p1_rdd1.filter(x => checkYear(x._2) == p7_value1._1)
    val p7_rdd5: Array[(Int, (Double, String))] = p7_rdd2.join(p7_rdd4).sortBy(_._2._1, false).take(10)
    p7_rdd5.foreach(println)


    context.stop()
  }
}
