package sparkSql

import org.apache.log4j.{Level, Logger}
import utils.{Context, Utils}

object RddDataSetConversion extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val responseData = sparkSession.sparkContext.textFile("in/2016-stack-overflow-survey-responses.csv")

  val response = responseData.filter(line => !line.split(Utils.COMMA_DELIMITER, -1)(2).equals("country"))
    .map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER, -1)
      Response(splits(2), toInt(splits(6)), splits(9), toInt(splits(14)))
    })

  import sparkSession.implicits._

  val responseDataSet = response.toDS()
  responseDataSet.printSchema()

  responseDataSet.show(20)

  response.collect().foreach(println)

  def toInt(split: String): Option[Double] = {
    if (split.isEmpty) None else Some(split.toDouble)
  }
}
