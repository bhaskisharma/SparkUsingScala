package pairrdd.aggregation.reduceByKey.HouseProblem

import org.apache.log4j.{Level, Logger}
import utils.Context

/* Create a Spark program to read the house data from in/RealEstate.csv,
    output the average price for houses with different number of bedrooms.
 The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
 around it.
 The dataset contains the following fields:
 1. MLS: Multiple listing service number for the house (unique ID).
 2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
 northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
 some out of area locations as well.
 3. Price: the most recent listing price of the house (in dollars).
 4. Bedrooms: number of bedrooms.
 5. Bathrooms: number of bathrooms.
 6. Size: size of the house in square feet.
 7. Price/SQ.ft: price of the house per square foot.
 8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.
 Each field is comma separated.
 Sample output:
    (3, 325000)
    (1, 266356)
    (2, 325000)
    ...
    3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
  */
object AverageHousePriceProblem extends App with Context {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val lines = sparkSession.sparkContext.textFile("in/RealEstate.csv")

  val cleanLines = lines.filter(line => !line.contains("Bedrooms"))

  val housePricePairRdd = cleanLines.map(line =>
    (line.split(",")(3).toInt, AvgCount(1, line.split(",")(2).toDouble)))

  val housePriceTotal = housePricePairRdd.reduceByKey((x, y) => AvgCount(x.count + y.count, x.total + y.total))

  housePriceTotal.foreach(println)

  val housePriceAvg = housePriceTotal.mapValues(avgCount => avgCount.total / avgCount.count)

 val sortedByKey =  housePriceAvg.sortByKey(ascending = false)

  sortedByKey.collect().foreach(println)

}
