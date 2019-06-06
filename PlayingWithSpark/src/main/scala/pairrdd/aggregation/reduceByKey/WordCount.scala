package pairrdd.aggregation.reduceByKey

import org.apache.log4j.{Level, Logger}
import utils.Context

object WordCount extends App with Context {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val lines = sparkSession.sparkContext.textFile("in/word_count.text")

  val wordRdd = lines.flatMap(line => line.split(" "))

  val wordPairRdd = wordRdd.map(line => (line,1)).reduceByKey((x,y)=> x+y)

  wordPairRdd.foreach(println)
}
