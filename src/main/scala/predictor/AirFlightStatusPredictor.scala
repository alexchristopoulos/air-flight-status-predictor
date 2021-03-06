package gr.upatras.ceid.ddcdm.predictor

import gr.upatras.ceid.ddcdm.predictor.classification.{GradientBoostedTreeClassification, MultiLayerPerceptonClassification, NaiveBayesClassification, RandomForestClassification, SVM}
import gr.upatras.ceid.ddcdm.predictor.datasets.TestDataset
import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import gr.upatras.ceid.ddcdm.predictor.preprocess.PrincipalComponentAnalysis
import gr.upatras.ceid.ddcdm.predictor.classification.Classification
import gr.upatras.ceid.ddcdm.predictor.prediction.{IsotonicPrediction, RFPrediction, LinearRegressionPrediction}
import gr.upatras.ceid.ddcdm.predictor.prediction.Prediction
import gr.upatras.ceid.ddcdm.predictor.spark.SparkClustering
import gr.upatras.ceid.ddcdm.predictor.preprocess.DatasetPreprocessing
import gr.upatras.ceid.ddcdm.predictor.preprocess.PCAReduction

object AirFlightStatusPredictor {

  def run(): Unit = {

    val startTime = System.nanoTime()

    Spark.getSparkContext().setLogLevel("ERROR")

    try {

      //    *** TRAIN MODELS ***
      Classification.trainAndOrTest(false, true, RandomForestClassification.RFClassifier)
      Classification.trainAndOrTest(false, true, MultiLayerPerceptonClassification.multilayerPercepton)
      Classification.trainAndOrTest(false, true, NaiveBayesClassification.naiveBayesClassifier)
      Prediction.trainAndOrTest(false, true, LinearRegressionPrediction.linearRegression)
      Prediction.trainAndOrTest(false, true, IsotonicPrediction.isotonicRegression)
      Prediction.trainAndOrTest(false, true, RFPrediction.rfRegression)

      //    *** TEST MODELS ***
      Classification.classify(RandomForestClassification.RFClassifier)
      Classification.classify(MultiLayerPerceptonClassification.multilayerPercepton)
      Classification.classify(NaiveBayesClassification.naiveBayesClassifier)
      Prediction.predict(LinearRegressionPrediction.linearRegression)
      Prediction.predict(IsotonicPrediction.isotonicRegression)
      Prediction.predict(RFPrediction.rfRegression)

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