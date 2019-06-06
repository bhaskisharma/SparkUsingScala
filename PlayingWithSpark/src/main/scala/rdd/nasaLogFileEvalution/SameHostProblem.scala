package rdd.nasaLogFileEvalution

import org.apache.log4j.{Level, Logger}
import utils.Context


/*    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
      "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
      Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
      Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.
      Example output:
      vagrant.vf.mmc.com
      www-a1.proxy.aol.com
      .....
      Keep in mind, that the original log files contains the following header lines.
      host	logname	time	method	url	response	bytes
      Make sure the head lines are removed in the resulting RDD.
    */
object SameHostProblem extends App with Context {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val julyLog = sparkSession.sparkContext.textFile("in/nasa_19950701.tsv.txt")

  val augustLog = sparkSession.sparkContext.textFile("in/nasa_19950801.tsv.txt")

  val julyFirstHosts = julyLog.map(line => line.split("\t")(0))
  val augustFirstHosts = augustLog.map(line => line.split("\t")(0))

  val resultRdd = julyFirstHosts.intersection(augustFirstHosts)

  val cleanUpHost = resultRdd.filter(line => line != "host")

  cleanUpHost.saveAsTextFile("out/Sample_Host.txt")

}
