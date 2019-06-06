package rdd.example

import org.apache.log4j.{Level, Logger}
import utils.Context

object WordCount extends App with Context {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val lines = sparkSession.sparkContext.textFile("in/word_count.text")

  val word = lines.flatMap(line => line.split(" "))
  val wordCount = word.countByValue()
  for ((word, count) <- wordCount) println(word + " - " + count)

}
