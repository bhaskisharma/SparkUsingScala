package utils

import org.apache.spark.sql.SparkSession

trait Context {

  System.setProperty("hadoop.home.dir", "C:/hadoop")

  lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("PlayingWithSpark")
    .config("spark.master", "local[2]")
    .config("spark.cores.max", "2")
    .getOrCreate()

}
