package advanced.accumulator

import org.apache.log4j.{Level, Logger}
import utils.{Context, Utils}

object StackOverFlowSurveyFollowUp extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val total = sparkSession.sparkContext.longAccumulator
  val missingSalaryMidPoint = sparkSession.sparkContext.longAccumulator
  val processBytes = sparkSession.sparkContext.longAccumulator

  val stackOverFlowSurvey = sparkSession.sparkContext.textFile("in/2016-stack-overflow-survey-responses.csv")

  val responseFromCanada = stackOverFlowSurvey.filter(line => {

    processBytes.add(line.getBytes().length)
    val splits = line.split(Utils.COMMA_DELIMITER, -1)
    total.add(1)

    if (splits(14).isEmpty) missingSalaryMidPoint.add(1)

    splits(2) == "Canada"
  })

  println("Count of responses from Canada: " + responseFromCanada.count())
  println("Number of bytes processed: " + processBytes.value)
  println("Total count of responses: " + total.value)
  println("Count of responses missing salary middle point: " + missingSalaryMidPoint.value)

}
