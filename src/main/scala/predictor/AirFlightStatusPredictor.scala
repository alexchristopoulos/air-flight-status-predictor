package gr.upatras.ceid.ddcdm.predictor

import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import gr.upatras.ceid.ddcdm.predictor.spark.SparkClustering

object AirFlightStatusPredictor {

  def run(): Unit = {

    Spark.getSparkContext()
    //val sparkClustering: SparkClustering = new SparkClustering(Spark.getSparkContext())
    //sparkClustering.kMeansClustering()
    //spark.exit()
    Spark.exit()
  }

}