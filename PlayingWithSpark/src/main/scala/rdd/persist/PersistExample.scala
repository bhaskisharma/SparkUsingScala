package rdd.persist

import org.apache.log4j.{Level, Logger}
import utils.Context
import org.apache.spark.storage.StorageLevel


object PersistExample extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val inputNumber = List(1, 2, 3, 4, 5)
  val rddNumber = sparkSession.sparkContext.parallelize(inputNumber)

  rddNumber.persist(StorageLevel.MEMORY_ONLY)

  rddNumber.reduce((x, y) => x + y)

  rddNumber.count()
}
