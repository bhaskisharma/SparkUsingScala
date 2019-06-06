package rdd.ActionsInSpark

import org.apache.log4j.{Level, Logger}
import rdd.ActionsInSpark.Counts.sparkSession
import utils.Context

object ReduceR extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val inputIntegers = List(1, 2, 3, 4, 5)

  val rddCount = sparkSession.sparkContext.parallelize(inputIntegers)

  val product = rddCount.reduce((x , y) => x*y)

  println("products :   :"+product)
}
