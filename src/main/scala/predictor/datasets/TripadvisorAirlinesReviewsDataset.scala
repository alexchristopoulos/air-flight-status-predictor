package gr.upatras.ceid.ddcdm.predictor.datasets

import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import gr.upatras.ceid.ddcdm.predictor.util.FuncOperators

object TripadvisorAirlinesReviewsDataset {

  private var datasetRdd: RDD[Row] = _
  private var datasetDf: DataFrame = _

  val struct = StructType(
    StructField("iata", StringType, false) ::
      StructField("name", StringType, false) ::
      StructField("rating", StringType, false) ::
      StructField("numOfReviews", StringType, false) :: Nil)

  def load(sparkContext: SparkContext, sparkSession: SparkSession): Unit = {

    this.datasetRdd = sparkContext
      .textFile(config.sparkDatasetDir + config.sparkDatasetTripadvisorAirlinesReviews)
      .mapPartitionsWithIndex(FuncOperators.removeFirstLine)
      .map(FuncOperators.csvStringRowToRow)

    this.datasetDf = sparkSession.createDataFrame(this.datasetRdd, this.struct)
    this.datasetDf.as("airlinesReviews")
    this.datasetDf.createOrReplaceTempView("airlinesReviews")

  }

  def getAsDf(): DataFrame = {

    return this.datasetDf
  }

  def getAsRdd(): RDD[Row] = {

    return this.datasetRdd
  }


}
