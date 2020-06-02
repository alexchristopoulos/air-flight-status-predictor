package gr.upatras.ceid.ddcdm.predictor.classification

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

object MultiLayerPerceptonClassification {

  val multilayerPercepton: MultilayerPerceptronClassifier = new MultilayerPerceptronClassifier()
    .setLabelCol("CANCELLED")
    .setMaxIter(280)
    .setTol(1E-7)
    //.setLayers(Array(6,14,14,14,2))
    .setLayers(Array(12,20,10,6,2))
}