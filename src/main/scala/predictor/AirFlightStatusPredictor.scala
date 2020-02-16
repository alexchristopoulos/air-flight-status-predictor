package gr.upatras.ceid.ddcdm.predictor

import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import gr.upatras.ceid.ddcdm.predictor.spark.SparkClustering
class AirFlightStatusPredictor {

  def AirFlightStatusPredictor(): Unit = {}

  def run(): Unit = {

    var spark: Spark = new Spark()

    println("Worked!!!!!!!")

    //val sparkClustering: SparkClustering = new SparkClustering(spark.getSparkContext())

    //sparkClustering.kMeansClustering()

    //spark.exit()
  }

}