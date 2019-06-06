package pairrdd.create

import org.apache.log4j.{Level, Logger}
import utils.Context

object PairRddFromTupleList extends App with Context{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val tuple = List(("Lily", 23), ("Jack", 29), ("Mary", 29), ("James", 8))
  val pairRDD = sparkSession.sparkContext.parallelize(tuple)

  pairRDD.coalesce(1).saveAsTextFile("out/Pair_RDD_TUPLES.text")
}
