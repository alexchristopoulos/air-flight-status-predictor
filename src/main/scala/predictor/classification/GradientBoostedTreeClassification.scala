package gr.upatras.ceid.ddcdm.predictor.classification

import org.apache.spark.ml.classification.{GBTClassifier}

object GradientBoostedTreeClassification {

  val gradientBoostedTree = new GBTClassifier()
    .setLabelCol("CANCELLED")

}
