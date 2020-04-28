package gr.upatras.ceid.ddcdm.predictor

import gr.upatras.ceid.ddcdm.predictor.classification.{NaiveBayesClassification, RandomForestClassification}
import gr.upatras.ceid.ddcdm.predictor.datasets.TestDataset
import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import gr.upatras.ceid.ddcdm.predictor.spark.SparkClustering
import gr.upatras.ceid.ddcdm.predictor.preprocess.DatasetPreprocessing
import gr.upatras.ceid.ddcdm.predictor.classification.GradientBoostedTreeClassification

object AirFlightStatusPredictor {

  def run(): Unit = {

    val startTime = System.nanoTime()

    Spark.getSparkContext().setLogLevel("ERROR")

    try {

      //RandomForestClassification.trainModel(false, true)
      //TestDataset.load()
      NaiveBayesClassification.trainModel(false, true)
      //RandomForestClassification.predict("TEST_FLIGHTS_DATA")
    } catch {
      case x:Exception => {
        println(x.toString())
        x.printStackTrace()
      }
    }

    //val sparkClustering: SparkClustering = new SparkClustering(Spark.getSparkContext())
    //sparkClustering.kMeansClustering()
    //spark.exit()
    Spark.exit()
    println("*** END AFTER " + ((System.nanoTime() - startTime).toFloat/60000000000f ).toString() + " MINUTES ****")
  }

}