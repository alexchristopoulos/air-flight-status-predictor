package gr.upatras.ceid.ddcdm.predictor.datasets

import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.sql.types.{StringType, StructField, StructType, IntegerType}
import gr.upatras.ceid.ddcdm.predictor.util.FuncOperators
import gr.upatras.ceid.ddcdm.predictor.spark.Spark

object Airports {

  private var datasetRdd: RDD[Row] = _
  private var datasetDf: DataFrame = _
  private var airportsAvgDepDelayDf = _
  private var isLoaded: Boolean = false
//ID,IATA_CODE,AIRPORT,CITY,STATE,COUNTRY,LATITUDE,LONGITUDE
  private val struct = StructType(
    StructField("id", IntegerType, false) ::
      StructField("iata", StringType, false) ::
      StructField("airport", StringType, false) ::
      StructField("city", StringType, false) ::
      StructField("state", StringType, false) ::
      StructField("country", StringType, false) ::
     // StructField("lat", StringType, true) ::
     // StructField("long", StringType, true) ::
      Nil)

  def load(): Unit = {

    this.datasetRdd = Spark
      .getSparkContext()
      .textFile(config.sparkDataResources + config.sparkAirports)
      .mapPartitionsWithIndex(FuncOperators.removeFirstLine)
      .map(line => FuncOperators.csvStringRowToRowType(line, Map(
        0 -> "Int",
        1 -> "String",
        2 -> "String",
        3 -> "String",
        4 -> "String",
        5 -> "String"
      )))

    this.datasetDf = Spark
      .getSparkSession()
      .createDataFrame(this.datasetRdd, this.struct)

    this.datasetDf.as("airports")
    this.datasetDf.createOrReplaceTempView("airports")
    this.isLoaded = true
  }

  def getAsDf(): DataFrame = { return this.datasetDf }
  def getAsRdd(): RDD[Row] = { return this.datasetRdd }
  def isItLoaded(): Boolean = { return this.isLoaded }

def avgDepDelayResourceExists(): Boolean = { return new File(config.sparkDataResources + config.sparkAirportsAvgDepDelay).exists() }

def saveAvgDepDelays(avgDepDelaysDf: DataFrame): Unit = {

  avgDepDelaysDf
    .repartition(1)
    .write
    .format("csv")
    .option("header", true)
    .option("sep", ",")
    .save(config.sparkDataResources + config.sparkAirportsAvgDepDelay)
}

def loadAvgDepDelays(): Unit = {

  val dir = config.sparkDataResources + config.sparkAirportsAvgDepDelay

  Spark
    .getSparkSession()
    .read
    .format("csv")
    .option("header", true)
    .load(dir)
    .withColumn("ORIGIN", col("ORIGIN").cast("Integer"))
    .withColumn("ORIGIN_AVG_DEP_DELAY", col("ORIGIN_AVG_DEP_DELAY").cast("Double"))
    .createOrReplaceTempView("ORIGN_AVG_DEPARTURE_DELAYS")
}

}
