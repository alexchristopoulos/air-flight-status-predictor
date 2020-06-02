package gr.upatras.ceid.ddcdm.predictor.classification

import org.apache.spark.ml.classification.LinearSVC

object SVM {

  val linearSupportVectorMachine = new LinearSVC()
    .setLabelCol("CANCELLED")
    .setMaxIter(20)

}
