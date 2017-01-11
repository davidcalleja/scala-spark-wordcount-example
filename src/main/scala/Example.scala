package example

import org.apache.spark.{SparkConf, SparkContext}

object Example extends App {

  override def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Count")
    val sc = new SparkContext(conf)

    val rdd = {
      sc.textFile("/home/david/Documents/workspace/Scala_course/untitled/src/main/resources/guion.txt")
        .flatMap(_.split(" "))
        .filter(_.matches("[a-zA-Z0-9]+"))
        .map(word => (word.toLowerCase, 1))
        .reduceByKey(_ + _)
    }

    val prepositions = {
      sc.textFile("/home/david/Documents/workspace/Scala_course/untitled/src/main/resources/prepositions.txt")
        .flatMap(x => List(x)).collect()
    }

    val mostWritten =
      rdd.filter(word => !prepositions.contains(word._1)).takeOrdered(1)(Ordering[Int].reverse.on(_._2))

    println(mostWritten mkString "")
  }
}