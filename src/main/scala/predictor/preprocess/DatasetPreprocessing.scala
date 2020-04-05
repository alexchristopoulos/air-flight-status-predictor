package gr.upatras.ceid.ddcdm.predictor.preprocess

import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import gr.upatras.ceid.ddcdm.predictor.config.config
import gr.upatras.ceid.ddcdm.predictor.datasets._
import java.io._

import gr.upatras.ceid.ddcdm.predictor.datasets.AirFlightsKaggleDataset

object DatasetPreprocessing {

  private val ss = Spark.getSparkSession()

  def preprocess(): Unit = {

    this.combineAirlinesWithTripadvisorReviews()
  }


  def combineAirlinesWithTripadvisorReviews(): Unit = {

    TripadvisorAirlinesReviewsDataset.load()
    Airlines.load()

    val bufferedWriter: BufferedWriter = new BufferedWriter(new FileWriter(config.sparkOutputDataset + "airlinesWithTripAdvisorReviews.out"))

    this.ss
      .sql("SELECT ar.id, a.iata, a.name, ar.rating, ar.numOfReviews FROM airlines as a INNER JOIN  airlinesReviews AS ar ON a.iata=ar.iata")
      .rdd
      .collect()
      .foreach(row => {

        bufferedWriter.write(row(0).toString() + "," + row(1).toString() + "," + row(2).toString() + "," + row(3).toString() + "," + row(4).toString())
        bufferedWriter.newLine()
      })

    bufferedWriter.close()
  }
}