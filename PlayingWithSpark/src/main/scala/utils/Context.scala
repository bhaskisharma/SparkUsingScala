package utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {

  lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("PlayingWithSpark")
    .config("spark.master", "local[*]")
    .config("spark.cores.max", "2")
    .getOrCreate()

}
