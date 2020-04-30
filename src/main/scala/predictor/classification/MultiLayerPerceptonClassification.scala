package gr.upatras.ceid.ddcdm.predictor.classification

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

object MultiLayerPerceptonClassification {

  val multilayerPercepton: MultilayerPerceptronClassifier = new MultilayerPerceptronClassifier()
    .setLabelCol("CANCELLED")
    .setMaxIter(200)
    .setTol(1E-5)
    .setLayers(Array(12,8,5,2))

}
