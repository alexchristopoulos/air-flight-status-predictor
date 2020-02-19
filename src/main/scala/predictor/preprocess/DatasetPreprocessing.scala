package gr.upatras.ceid.ddcdm.predictor.preprocess

import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions
import java.io._

object DatasetPreprocessing {

  private val sc = Spark.getSparkContext()
  private val ss = Spark.getSparkSession()

  def combineAirlinesWithTripadvisorReviews(): Unit = {

    //load dataset and remove first line
    val airlinesRdd = sc
      .textFile(config.sparkDatasetDir + config.sparkDatasetPredictionAirlines)
      .mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter}
      .map(line => Row.fromSeq(line.split(",").toSeq))

    //load dataset and remove first line
    val airlinesReviewsRdd = sc
      .textFile(config.sparkDatasetDir + config.sparkDatasetTripadvisorAirlinesReviews)
      .mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter}
      .map(line => Row.fromSeq(line.split(",").toSeq))

    val structAirlines = StructType(
      StructField("iata1", StringType, false) ::
        StructField("name1", StringType, false) :: Nil)

    val structAirlinesReviews = StructType(
      StructField("iata2", StringType, false) ::
        StructField("name2", StringType, false) ::
        StructField("rating", StringType, false) ::
        StructField("numOfReviews", StringType, false) :: Nil)

    val dfAirlines = this.ss.createDataFrame(airlinesRdd, structAirlines)
    val dfAirlinesReviews = this.ss.createDataFrame(airlinesReviewsRdd, structAirlinesReviews)

    dfAirlines.as("airlines")
    dfAirlinesReviews.as("airlinesReviews")

    val res = dfAirlines.join(dfAirlinesReviews, functions.col("iata1") === functions.col("iata2"), "inner").rdd.collect()

    val bufferedWriter: BufferedWriter = new BufferedWriter(new FileWriter(config.sparkOutputDataset + "airlinesWithTripAdvisorReviews.out"))

    res.foreach(row => {
      bufferedWriter.write(row.get(0).toString() + "," + row.get(1).toString() + "," + row.get(4).toString() + "," + row.get(5).toString())
      bufferedWriter.newLine()
    })

    bufferedWriter.close()
  }

}