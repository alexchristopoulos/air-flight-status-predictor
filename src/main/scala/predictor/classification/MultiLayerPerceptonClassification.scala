package gr.upatras.ceid.ddcdm.predictor.classification

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

object MultiLayerPerceptonClassification {

  val multilayerPercepton: MultilayerPerceptronClassifier = new MultilayerPerceptronClassifier()
    .setLabelCol("CANCELLED")
    .setMaxIter(215)
    .setTol(1E-7)
    .setLayers(Array(12,16,8,2))
    //.setLayers(Array(12,24,14,7,2))
    //.setLayers(Array(12,48,8,2))

}

/**
.setMaxIter(200)
    .setTol(1E-5)
    .setLayers(Array(12,8,5,2))
  */