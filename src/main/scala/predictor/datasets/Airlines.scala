package gr.upatras.ceid.ddcdm.predictor.datasets

import java.io.File

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import gr.upatras.ceid.ddcdm.predictor.util.FuncOperators
import gr.upatras.ceid.ddcdm.predictor.spark.Spark

object Airlines {

  private var datasetRdd: RDD[Row] = _
  private var datasetDf: DataFrame = _
  private var isLoaded: Boolean = false

  private val struct = StructType(
    StructField("id", IntegerType, false) ::
      StructField("iata", StringType, false) ::
      StructField("name", StringType, false) :: Nil)

  def load(): Unit = {

    this.datasetRdd = Spark
      .getSparkContext()
      .textFile(config.sparkDataResources + config.sparkAirlines)
      .mapPartitionsWithIndex(FuncOperators.removeFirstLine)
      .map(line => FuncOperators.csvStringRowToRowType(line, Map(
        0 -> "Int",
        1 -> "String",
        2 -> "String"
      )))

    this.datasetDf = Spark
      .getSparkSession()
      .createDataFrame(this.datasetRdd, this.struct)

    this.datasetDf.as("airlines")
    this.datasetDf.createOrReplaceTempView("airlines")

    this.isLoaded = true
  }

  def getAsDf(): DataFrame = { return this.datasetDf }
  def getAsRdd(): RDD[Row] = { return this.datasetRdd }
  def isItLoaded(): Boolean = { return this.isLoaded  }
  def avgDelayResourcesExist(): Boolean = { return new File(config.sparkDataResources + config.sparkAirlinesAVGDel).exists() }

  def saveAvgDelays(avgDelaysDf: DataFrame): Unit = {

    avgDelaysDf
      .repartition(1)
      .write
      .format("csv")
      .option("header", true)
      .option("sep", ",")
      .save(config.sparkDataResources + config.sparkAirlinesAVGDel)
  }

  def loadAvgDelays(): Unit = {

    val dir = config.sparkDataResources + config.sparkAirlinesAVGDel

    Spark
      .getSparkSession()
      .read
      .format("csv")
      .option("header", true)
      .load(dir)
      .withColumn("OP_CARRIER_ID", col("OP_CARRIER_ID").cast("Integer"))
      .withColumn("AIRLINE_MEAN_DELAY", col("AIRLINE_MEAN_DELAY").cast("Double"))
      .createOrReplaceTempView("MEAN_DELAY_AIRLINES")
  }

}
