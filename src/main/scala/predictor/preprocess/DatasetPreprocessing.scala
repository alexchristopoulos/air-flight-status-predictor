package gr.upatras.ceid.ddcdm.predictor.preprocess

import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import gr.upatras.ceid.ddcdm.predictor.config.config
import gr.upatras.ceid.ddcdm.predictor.datasets._
import java.io._

import gr.upatras.ceid.ddcdm.predictor.datasets.AirFlightsKaggleDataset

object DatasetPreprocessing {

  private val sc = Spark.getSparkContext()
  private val ss = Spark.getSparkSession()

  def preprocess(): Unit = {

    //this.combineAirlinesWithTripadvisorReviews()
    this.preprocessFlights2015Dataset()
  }

  def preprocessFlights2015Dataset(): Unit = {

    val start = System.nanoTime()
    AirFlightsKaggleDataset.load(this.sc, this.ss)

    //AirFlightsKaggleDataset.getAsDf().show(150)


    println(System.nanoTime() - start)
  }

  def combineAirlinesWithTripadvisorReviews(): Unit = {

    TripadvisorAirlinesReviewsDataset.load(this.sc, this.ss)
    AirlinesDataset.load(this.sc, this.ss)

    val bufferedWriter: BufferedWriter = new BufferedWriter(new FileWriter(config.sparkOutputDataset + "airlinesWithTripAdvisorReviews.out"))

    this.ss
      .sql("SELECT a.iata, a.name, ar.rating, ar.numOfReviews FROM airlines as a INNER JOIN  airlinesReviews AS ar ON a.iata=ar.iata")
      .rdd
      .collect()
      .foreach(row => {

        bufferedWriter.write(row(0).toString() + "," + row(1).toString() + "," + row(2).toString() + "," + row(3).toString())
        bufferedWriter.newLine()
      })

    bufferedWriter.close()
  }
}