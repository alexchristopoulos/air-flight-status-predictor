package gr.upatras.ceid.ddcdm.predictor.datasets

import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import gr.upatras.ceid.ddcdm.predictor.util.FuncOperators
import gr.upatras.ceid.ddcdm.predictor.spark.Spark

object AirlinesDataset {

  private var datasetRdd: RDD[Row] = _
  private var datasetDf: DataFrame = _

  private val struct = StructType(
    StructField("id", StringType, false) ::
    StructField("iata", StringType, false) ::
      StructField("name", StringType, false) :: Nil)

  def load(): Unit = {

    this.datasetRdd = Spark
      .getSparkContext()
      .textFile(config.sparkDatasetDir + config.sparkDatasetPredictionAirlines)
      .mapPartitionsWithIndex(FuncOperators.removeFirstLine)
      .map(FuncOperators.csvStringRowToRow)

    this.datasetDf = Spark
      .getSparkSession()
      .createDataFrame(this.datasetRdd, this.struct)

    this.datasetDf.as("airlines")
    this.datasetDf.createOrReplaceTempView("airlines")
  }

  def getAsDf(): DataFrame = {

    return this.datasetDf
  }

  def getAsRdd(): RDD[Row] = {

    return this.datasetRdd
  }

}
