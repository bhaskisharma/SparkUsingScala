package rdd.airports

import utils.{Context, Utils}


/* Create a Spark program to read the airport data from in/airports.text,
          find all the airports whose latitude are bigger than 40.
          Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.
          Each row of the input file contains the following columns:
          Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
          ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
          Sample output:
          "St Anthony", 51.391944
          "Tofino", 49.082222
          ...
        */
object AirportsByLatitudeProblem extends App with Context {

  val airports = sparkSession.sparkContext.textFile("in/airports.text")

  val airportLat = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)

  val airportLatAndName = airportLat.map(line => {
    val split = line.split(Utils.COMMA_DELIMITER)
    split(1) + " , " + split(6)
  })

  airportLatAndName.saveAsTextFile("out/airportLatName.text")

}
