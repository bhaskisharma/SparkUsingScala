package sparkSql

import org.apache.log4j.{Level, Logger}
import utils.Context

object StackOverFlowSurveyUsingSQL extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val AGE_MIDPOINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

  val dataFrameRead = sparkSession.read

  val response = dataFrameRead.option("header", "true")
    .option("inferSchema", value = true)
    .csv("in/2016-stack-overflow-survey-responses.csv")

  println("  == print out the schema == ")

  response.printSchema()

  val responseWithSelectedColumns = response.select("country","occupation",AGE_MIDPOINT,SALARY_MIDPOINT)

  println("=== Print the selected columns of the table ===")
  responseWithSelectedColumns.show()

  println("  == Print records where the response is from Afghanistan == ")
  responseWithSelectedColumns.filter(responseWithSelectedColumns.col("country").===("Afghanistan")).show()

  println("  == Print the count of occupations == ")
  val groupDataSet = responseWithSelectedColumns.groupBy("occupation")
  groupDataSet.count().show()

  println("  == Print records with average mid age less than 20 == ")

  responseWithSelectedColumns.filter(responseWithSelectedColumns.col(AGE_MIDPOINT) < 20).show()

  println("  == Print the result by salary middle point in descending order == ")

  responseWithSelectedColumns.orderBy(responseWithSelectedColumns.col(SALARY_MIDPOINT).desc).show()

  println("=== Group by country and aggregate by average salary middle point ===")
  val groupDatS = responseWithSelectedColumns.groupBy("country")
  groupDatS.avg(SALARY_MIDPOINT).show()

  val responseWithSalaryBucket = response.withColumn(SALARY_MIDPOINT_BUCKET,response.col(SALARY_MIDPOINT).
    divide(20000).cast("integer").multiply(20000))

  println("=== With salary bucket column ===")
  responseWithSalaryBucket.select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET).show()

  println("=== Group by salary bucket ===")
  responseWithSalaryBucket.groupBy(SALARY_MIDPOINT_BUCKET).count().orderBy(SALARY_MIDPOINT_BUCKET).show()

  sparkSession.stop()
}
