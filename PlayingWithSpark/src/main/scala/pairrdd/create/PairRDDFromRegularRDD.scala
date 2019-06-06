package pairrdd.create

import org.apache.log4j.{Level, Logger}
import utils.Context

object PairRDDFromRegularRDD extends App with Context{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val tuple = List("Lily 23", "Jack 29", "Mary 29", "James 8")
  val regularRDD = sparkSession.sparkContext.parallelize(tuple)

  val pairRDD = regularRDD.map(s => (s.split(" ")(0), s.split(" ")(1)))

  pairRDD.coalesce(1).saveAsTextFile("out/Pair_RDD_FROM_RDD.text")
}
