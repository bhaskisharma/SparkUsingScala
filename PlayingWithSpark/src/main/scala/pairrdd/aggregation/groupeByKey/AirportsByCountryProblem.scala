package pairrdd.aggregation.groupeByKey

import org.apache.log4j.{Level, Logger}
import utils.{Context, Utils}

/* Create a Spark program to read the airport data from in/airports.text,
      output the the list of the names of the airports located in each country.
      Each row of the input file contains the following columns:
      Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
      ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
      Sample output:
      "Canada", List("Bagotville", "Montreal", "Coronation", ...)
      "Norway" : List("Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..)
      "Papua New Guinea",  List("Goroka", "Madang", ...)
      ...
    */

object AirportsByCountryProblem extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val lines = sparkSession.sparkContext.textFile("in/airports.text")

  val airportInCountry = lines.map(line => (line.split(Utils.COMMA_DELIMITER)(3),
    line.split(Utils.COMMA_DELIMITER)(1)))

  val airportCountryWithName = airportInCountry.groupByKey()

  airportCountryWithName.foreach(println)

}
