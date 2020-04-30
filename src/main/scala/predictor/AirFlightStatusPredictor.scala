package gr.upatras.ceid.ddcdm.predictor

import gr.upatras.ceid.ddcdm.predictor.classification.{GradientBoostedTreeClassification, MultiLayerPerceptonClassification, NaiveBayesClassification, RandomForestClassification}
import gr.upatras.ceid.ddcdm.predictor.datasets.TestDataset
import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import gr.upatras.ceid.ddcdm.predictor.classification.Classification
import gr.upatras.ceid.ddcdm.predictor.spark.SparkClustering
import gr.upatras.ceid.ddcdm.predictor.preprocess.DatasetPreprocessing

object AirFlightStatusPredictor {

  def run(): Unit = {

    val startTime = System.nanoTime()

    Spark.getSparkContext().setLogLevel("ERROR")

    try {

      //train models
      //Classification.trainAndOrTest(false, true, RandomForestClassification.RFClassifier)
      //Classification.trainAndOrTest(false, true, GradientBoostedTreeClassification.gradientBoostedTree)
      //Classification.trainAndOrTest(false, true, MultiLayerPerceptonClassification.multilayerPercepton)
      //Classification.trainAndOrTest(false, true, NaiveBayesClassification.naiveBayesClassifier)

      //test models
      //Classification.classify(RandomForestClassification.RFClassifier)
      //Classification.classify(GradientBoostedTreeClassification.gradientBoostedTree)
      Classification.classify(MultiLayerPerceptonClassification.multilayerPercepton)
      //Classification.classify(NaiveBayesClassification.naiveBayesClassifier)

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