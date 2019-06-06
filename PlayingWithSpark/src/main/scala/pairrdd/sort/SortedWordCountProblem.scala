package pairrdd.sort

import org.apache.log4j.{Level, Logger}
import utils.Context


/* Create a Spark program to read the an article from in/word_count.text,
      output the number of occurrence of each word in descending order.
      Sample output:
      apple : 200
      shoes : 193
      bag : 176
      ...
    */
object SortedWordCountProblem extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val lines = sparkSession.sparkContext.textFile("in/word_count.text")
  val wordMap = lines.flatMap(line => line.split(" "))
  val pairRdd = wordMap.map(word => (word, 1)).reduceByKey((x, y) => x + y)

  val countToWordParis = pairRdd.map(wordToCount => (wordToCount._2, wordToCount._1))

  val sortedCountToWordParis = countToWordParis.sortByKey(ascending = false)

  val sortedWordToCountPairs = sortedCountToWordParis.map(countToWord => (countToWord._2, countToWord._1))

  sortedWordToCountPairs.collect().foreach(println)
}
