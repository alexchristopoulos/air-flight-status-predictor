package gr.upatras.ceid.ddcdm.predictor.datasets

import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
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

  def getAsDf(): DataFrame = {

    return this.datasetDf
  }

  def getAsRdd(): RDD[Row] = {

    return this.datasetRdd
  }

  def isItLoaded(): Boolean = {
    return this.isLoaded
  }

}
