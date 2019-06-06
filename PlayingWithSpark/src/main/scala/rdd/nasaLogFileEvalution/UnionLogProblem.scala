package rdd.nasaLogFileEvalution

import org.apache.log4j.{Level, Logger}
import utils.Context

/* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
  "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
  Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
  take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.
  Keep in mind, that the original log files contains the following header lines.
  host	logname	time	method	url	response	bytes
  Make sure the head lines are removed in the resulting RDD.
*/
object UnionLogProblem extends App with Context {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val julyLog = sparkSession.sparkContext.textFile("in/nasa_19950701.tsv.txt")
  val augustLog = sparkSession.sparkContext.textFile("in/nasa_19950801.tsv.txt")

  val julyAndAugust = julyLog.union(augustLog)

  val cleanHeadlines = julyAndAugust.filter(line => isNotHeader(line))

  val sampleRdd = cleanHeadlines.sample(true, 0.1)
  sampleRdd.saveAsTextFile("out/july_and_august.tsv.txt")

  def isNotHeader(line: String): Boolean = !(line.startsWith("host") && line.contains("bytes"))
}



