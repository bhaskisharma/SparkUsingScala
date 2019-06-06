package rdd.SumOfNumberProblem

import org.apache.log4j.{Level, Logger}
import utils.Context


/* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.
       Each row of the input file contains 10 prime numbers separated by spaces.
     */
object SumOfNumberProblem extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val primeRead = sparkSession.sparkContext.textFile("in/prime_nums.text")

  val number = primeRead.flatMap(line => line.split("\\s+"))

  val validNumber = number.filter(line => !line.isEmpty)

  val numberInt = validNumber.map(line => line.toInt).reduce((x,y) => x + y)

  println("Sum of prime number : "+numberInt)
}
