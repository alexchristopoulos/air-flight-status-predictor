package gr.upatras.ceid.ddcdm.predictor

import gr.upatras.ceid.ddcdm.predictor.classification.RandomForestClassification
import gr.upatras.ceid.ddcdm.predictor.datasets.TestDataset
import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import gr.upatras.ceid.ddcdm.predictor.spark.SparkClustering
import gr.upatras.ceid.ddcdm.predictor.preprocess.DatasetPreprocessing

object AirFlightStatusPredictor {

  def run(): Unit = {

    val startTime = System.nanoTime()

    Spark.getSparkContext().setLogLevel("ERROR")

    try {

      println("***************************")
      println("RANDOM FOREST TEST")

      //RandomForestClassification.trainModel(false, true)
      TestDataset.load()
      RandomForestClassification.predict("TEST_FLIGHTS_DATA")

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