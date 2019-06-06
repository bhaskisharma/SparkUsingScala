package pairrdd.mapValues

import org.apache.log4j.{Level, Logger}
import utils.{Context, Utils}

/* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
       being the key and country name being the value. Then convert the country name to uppercase and
       output the pair RDD to out/airports_uppercase.text
       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
       Sample output:
       ("Kamloops", "CANADA")
       ("Wewak Intl", "PAPUA NEW GUINEA")
       ...
     */

object AirportsUppercaseProblem extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val airports = sparkSession.sparkContext.textFile("in/airports.text")

  val airportNameAndCountry = airports.map(line => (line.split(Utils.COMMA_DELIMITER)(1),
    line.split(Utils.COMMA_DELIMITER)(3)))

  val countryNameToUpperCase = airportNameAndCountry.mapValues(line => line.toUpperCase)

  countryNameToUpperCase.saveAsTextFile("out/airports_uppercase.text")
}
