package gr.upatras.ceid.ddcdm.predictor.datasets

import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import gr.upatras.ceid.ddcdm.predictor.util.FuncOperators

object AirlinesDataset {

  private var datasetRdd: RDD[Row] = _
  private var datasetDf: DataFrame = _

  private val struct = StructType(
    StructField("iata1", StringType, false) ::
      StructField("name1", StringType, false) :: Nil)

  def load(sparkContext: SparkContext, sparkSession: SparkSession): Unit = {

    this.datasetRdd = sparkContext
      .textFile(config.sparkDatasetDir + config.sparkDatasetPredictionAirlines)
      .mapPartitionsWithIndex(FuncOperators.removeFirstLine)
      .map(FuncOperators.csvStringRowToRow)

    this.datasetDf = sparkSession.createDataFrame(this.datasetRdd, this.struct)

    this.datasetDf.as("airlines")
  }

  def getAsDf(): DataFrame = {

    return this.datasetDf
  }

  def getAsRdd(): RDD[Row] = {

    return this.datasetRdd
  }

}
