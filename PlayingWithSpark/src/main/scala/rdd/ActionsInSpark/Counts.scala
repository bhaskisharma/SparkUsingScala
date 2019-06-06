package rdd.ActionsInSpark

import org.apache.log4j.{Level, Logger}
import utils.Context

object Counts extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")

  val rddCount = sparkSession.sparkContext.parallelize(inputWords)

  println(rddCount.count())

  val wordTake = rddCount.take(3)

  for (word <- wordTake) println(word)

  rddCount.countByValue().foreach(println)

}
