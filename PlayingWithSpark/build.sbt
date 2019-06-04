name := "PlayingWithSpark"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

scalacOptions in Compile ++= Seq("-deprecation", "-feature",
  "-unchecked", "-Xlog-reflective-calls", "-Xlint")
javacOptions in Compile ++= Seq("-Xlint:unchecked", "-Xlint:deprecation")
javaOptions in run ++= Seq("-Xms128m", "-Xmx3024m", "-Djava.library.path=./target/native")

libraryDependencies ++= {
  Seq(
    /**Spark Dependencies*/
    "org.apache.spark" %% "spark-core" % "2.2.0",
    "org.apache.spark" %% "spark-sql" % "2.2.0",
    "com.databricks" %% "spark-csv" % "1.5.0",
    "org.apache.spark" %% "spark-hive" % "2.2.0"
  )
}