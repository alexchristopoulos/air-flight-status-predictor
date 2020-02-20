package gr.upatras.ceid.ddcdm.predictor.datasets

import gr.upatras.ceid.ddcdm.predictor.config.config
import gr.upatras.ceid.ddcdm.predictor.preprocess.DatasetPreprocessing.sc
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TripadvisorAirlinesReviewsDataset {

  private var datasetRdd: RDD[Row] = _
  private var datasetDf: DataFrame = _

  val struct = StructType(
    StructField("iata2", StringType, false) ::
      StructField("name2", StringType, false) ::
      StructField("rating", StringType, false) ::
      StructField("numOfReviews", StringType, false) :: Nil)

  def load(sparkContext: SparkContext, sparkSession: SparkSession): Unit = {

    this.datasetRdd = sparkContext
      .textFile(config.sparkDatasetDir + config.sparkDatasetTripadvisorAirlinesReviews)
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter
      }.map(line => Row.fromSeq(line.split(",").toSeq))

    this.datasetDf = sparkSession.createDataFrame(this.datasetRdd, this.struct)
    this.datasetDf.as("airlinesReviews")

  }

  def getAsDf(): DataFrame = {

    return this.datasetDf
  }

  def getAsRdd(): RDD[Row] = {

    return this.datasetRdd
  }


}
