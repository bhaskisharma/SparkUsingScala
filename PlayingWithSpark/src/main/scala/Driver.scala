import utils.Context

object Driver extends App with Context {

  val dftags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tags")

  dftags.show(10)

  dftags.printSchema()

  dftags.count()

}
