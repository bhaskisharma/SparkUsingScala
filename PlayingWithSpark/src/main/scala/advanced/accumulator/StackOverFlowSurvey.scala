package advanced.accumulator

import org.apache.log4j.{Level, Logger}
import utils.{Context, Utils}


/**
  * How many records do we have in the survey result?
  * How many records are missing salary mid point?
  * How many records from canada?
  **/

object StackOverFlowSurvey extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val total = sparkSession.sparkContext.longAccumulator
  val missingSalaryMidPoint = sparkSession.sparkContext.longAccumulator

  val stackOverFlowSurvey = sparkSession.sparkContext.textFile("in/2016-stack-overflow-survey-responses.csv")

  val responseFromCanada = stackOverFlowSurvey.filter(line => {
    val splits = line.split(Utils.COMMA_DELIMITER, -1)
    total.add(1)

    if (splits(14).isEmpty) missingSalaryMidPoint.add(1)

    splits(2) == "Canada"
  })

  println("Count of responses from Canada: " + responseFromCanada.count())
  println("Total count of responses: " + total.value)
  println("Count of responses missing salary middle point: " + missingSalaryMidPoint.value)

}
