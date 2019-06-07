package sparkSql.join

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions
import utils.Context

object UkMakerSpacesSql extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val makerSpacesDf = sparkSession.read.option("header", "true")
    .csv("in/uk-makerspaces-identifiable-data.csv")

  val postcodeDf = sparkSession.read.option("header", "true")
    .csv("in/uk-postcode.csv").withColumn("PostCode",
    functions.concat_ws("", functions.col("PostCode"), functions.lit(" ")))

  println("=== Print 20 records of makerspace table ===")
  makerSpacesDf.select("Name of makerspace", "Postcode").show()


  println("=== Print 20 records of postcode table ===")
  postcodeDf.show()

  val joined = makerSpacesDf.join(postcodeDf, makerSpacesDf.col("Postcode").
    startsWith(postcodeDf.col("Postcode")), "left_outer")

  System.out.println("=== Group by Region ===")
  joined.groupBy("Region").count().show(200)

  sparkSession.close()

}
