package gr.upatras.ceid.ddcdm.predictor

import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import gr.upatras.ceid.ddcdm.predictor.spark.SparkClustering
import gr.upatras.ceid.ddcdm.predictor.preprocess.DatasetPreprocessing

object AirFlightStatusPredictor {

  def run(): Unit = {

    Spark.getSparkContext()
    try {
      DatasetPreprocessing.combineAirlinesWithTripadvisorReviews()
    } catch {
      case x:Exception => println(x.toString())
    }

    //val sparkClustering: SparkClustering = new SparkClustering(Spark.getSparkContext())
    //sparkClustering.kMeansClustering()
    //spark.exit()
    Spark.exit()
  }

}