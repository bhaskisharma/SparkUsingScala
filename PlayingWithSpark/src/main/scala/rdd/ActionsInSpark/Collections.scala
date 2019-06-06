package rdd.ActionsInSpark

import org.apache.log4j.{Level, Logger}
import utils.Context

object Collections extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")

  val rddCollect = sparkSession.sparkContext.parallelize(inputWords)

  rddCollect.collect().foreach(println)
}
