package rdd.airports

import org.apache.spark.{SparkConf, SparkContext}
import utils.{Context, Utils}


/* Create a Spark program to read the airport data from in/airports.text,
           find all the airports which are located in United States
           and output the airport's name and the city's name to out/airports_in_usa.text.
           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */
object AirportsInUSA extends App with Context {

  val airports = sparkSession.sparkContext.textFile("in/airports.text")

  val airportsInUSA = airports.filter(lines => lines.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"")

  airportsInUSA.foreach(println)

  val airportNameAndCity = airportsInUSA.map(line => {
    val splits = line.split(Utils.COMMA_DELIMITER)
    splits(1) + "," + splits(2)
  })

  airportNameAndCity.saveAsTextFile("out/airport_in_usa.txt")
}
