name := "air-flight-status-predictor"

version := "0.1"

scalaVersion := "2.12.6"

val sparkVersion = "2.4.4"

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
)

libraryDependencies += "org.jsoup" % "jsoup" % "1.8.3"