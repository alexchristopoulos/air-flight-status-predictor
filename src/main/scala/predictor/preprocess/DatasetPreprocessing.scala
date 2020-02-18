package gr.upatras.ceid.ddcdm.predictor.preprocess

import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.sql.types._

object DatasetPreprocessing {

  private val sc = Spark.getSparkContext()
  private val ss = Spark.getSparkSession()

  def combineAirlinesWithTripadvisorReviews(): Unit = {

    //load dataset and remove first line
    val airlinesRdd = sc.textFile(config.sparkDatasetDir + config.sparkDatasetPredictionAirlines).mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }.flatMap(line => line.split(","))

    //load dataset and remove first line
    val airlinesReviewsRdd = sc.textFile(config.sparkDatasetDir + config.sparkDatasetTripadvisorAirlinesReviews).mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }.flatMap(line => line.split(","))

    val structAirlines = StructType(
      StructField("iata", IntegerType, false) ::
        StructField("name", LongType, false) :: Nil)

    val structAirlinesReviews = StructType(
      StructField("iata", IntegerType, false) ::
        StructField("name", LongType, false) ::
        StructField("rating", BooleanType, false) ::
        StructField("numOfReviews", BooleanType, false) :: Nil)

    val dfAirlines = this.ss.createDataFrame(airlinesRdd, structAirlines)

  }

}