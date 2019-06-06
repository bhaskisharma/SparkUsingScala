package pairrdd.aggregation.groupeByKey

import org.apache.log4j.{Level, Logger}
import utils.Context

object GroupByKeyVsReduceByKey extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val words = List("one", "two", "two", "three", "three", "three")

  val pairRdd = sparkSession.sparkContext.parallelize(words).map(word => (word,1  ))

  val reduceByKeyData = pairRdd.reduceByKey((x, y) => x + y).collect()

  reduceByKeyData.foreach(println)

  val groupByKeyData = pairRdd.groupByKey().mapValues(x => x.size).collect()

  groupByKeyData.foreach(println)



}
