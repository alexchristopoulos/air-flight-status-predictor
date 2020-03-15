package gr.upatras.ceid.ddcdm.predictor.datasets

import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import gr.upatras.ceid.ddcdm.predictor.util.FuncOperators
import gr.upatras.ceid.ddcdm.predictor.spark.Spark

object TripadvisorAirlinesReviewsDataset {

  private var datasetRdd: RDD[Row] = _
  private var datasetDf: DataFrame = _

  val struct = StructType(
    StructField("iata", StringType, false) ::
      StructField("name", StringType, false) ::
      StructField("rating", DoubleType , false) ::
      StructField("numOfReviews", IntegerType, false) :: Nil)

  def load(): Unit = {

    this.datasetRdd = Spark
      .getSparkContext()
      .textFile(config.sparkDatasetDir + config.sparkDatasetTripadvisorAirlinesReviews)
      .mapPartitionsWithIndex(FuncOperators.removeFirstLine)
      .map(FuncOperators.csvStringRowToRowTripAdvReviews)

    this.datasetDf = Spark
      .getSparkSession()
      .createDataFrame(this.datasetRdd, this.struct)

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
