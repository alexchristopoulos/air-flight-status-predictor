package gr.upatras.ceid.ddcdm.predictor.datasets

import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import gr.upatras.ceid.ddcdm.predictor
import org.apache.spark.sql.functions
import java.io._

object DatasetPreprocessing {

  private val sc = Spark.getSparkContext()
  private val ss = Spark.getSparkSession()

  def combineAirlinesWithTripadvisorReviews(): Unit = {

    TripadvisorAirlinesReviewsDataset.load(this.sc, this.ss)
    AirlinesDataset.load(this.sc, this.ss)

    val dfAirlinesReviews = TripadvisorAirlinesReviewsDataset.getAsDf()
    val dfAirlines = AirlinesDataset.getAsDf()

    val res = dfAirlines.join(dfAirlinesReviews, functions.col("iata1") === functions.col("iata2"), "inner").rdd.collect()

    val bufferedWriter: BufferedWriter = new BufferedWriter(new FileWriter(config.sparkOutputDataset + "airlinesWithTripAdvisorReviews.out"))

    res.foreach(row => {
      bufferedWriter.write(row.get(0).toString() + "," + row.get(1).toString() + "," + row.get(4).toString() + "," + row.get(5).toString())
      bufferedWriter.newLine()
    })

    bufferedWriter.close()
  }

}