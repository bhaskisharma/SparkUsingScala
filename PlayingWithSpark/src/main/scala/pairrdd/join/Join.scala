package pairrdd.join

import org.apache.log4j.{Level, Logger}
import utils.{Context, Utils}

object Join extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val ages = sparkSession.sparkContext.parallelize(List(("Tom", 29),("John", 22)))
  val addresses = sparkSession.sparkContext.parallelize(List(("James", "USA"), ("John", "UK")))

  val joinRdd = ages.join(addresses)

  println("Inner Join ")
  joinRdd.collect().foreach(println)

  println("Left Join ")
  val leftJoin = ages.leftOuterJoin(addresses)

  leftJoin.collect().foreach(println)

  println("Right Outer Join ")
  val rightJoin = ages.rightOuterJoin(addresses)

  rightJoin.collect().foreach(println)

  println("Full Outer Join ")

  val fullOuter = ages.fullOuterJoin(addresses)

  fullOuter.collect().foreach(println)

}
