package advanced.broadcastr

import org.apache.log4j.{Level, Logger}
import utils.{Context, Utils}

import scala.io.Source


/**
  * How are those maker spaces distributed across different regions in the UK ?
  *
  * */
object UkMakerSpaces extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val postCodeData = sparkSession.sparkContext.broadcast(loadPostCodeMap())

  val makerSpaceRdd = sparkSession.sparkContext.textFile("in/uk-makerspaces-identifiable-data.csv")

  val regions = makerSpaceRdd.filter(line => line.split(Utils.COMMA_DELIMITER,-1)(0) != "Timestamp")
    .filter(line => getPostPrefix(line).isDefined)
    .map(line => postCodeData.value.getOrElse(getPostPrefix(line).get,"Unknown"))

  regions.countByValue().foreach(println)

  def getPostPrefix(line: String): Option[String] = {
    val splits = line.split(Utils.COMMA_DELIMITER, -1)
    val postcode = splits(4)
    if (postcode.isEmpty) None else Some(postcode.split(" ")(0))
  }

  def loadPostCodeMap() = {
    Source.fromFile("in/uk-postcode.csv").getLines().map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER, -1)
      splits(0) -> splits(7)
    }).toMap
  }
}
