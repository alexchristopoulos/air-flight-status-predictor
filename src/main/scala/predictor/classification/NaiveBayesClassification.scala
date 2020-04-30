package gr.upatras.ceid.ddcdm.predictor.classification

import org.apache.spark.ml.classification.NaiveBayes

object NaiveBayesClassification {

  val naiveBayesClassifier = new NaiveBayes()
    .setLabelCol("CANCELLED")

}
