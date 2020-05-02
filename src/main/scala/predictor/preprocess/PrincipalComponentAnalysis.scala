package gr.upatras.ceid.ddcdm.predictor.preprocess

import org.apache.spark.ml.feature.PCA

object PrincipalComponentAnalysis {


  def getPca() = {

    val pca = new PCA()
      .setInputCol("features")
      .setK(4)
      .setOutputCol("features")
  }
}
