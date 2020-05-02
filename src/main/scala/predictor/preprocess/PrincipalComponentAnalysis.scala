package gr.upatras.ceid.ddcdm.predictor.preprocess

import org.apache.spark.ml.feature.PCA

object PrincipalComponentAnalysis {


  def getPca(): PCA = {

     new PCA()
      .setInputCol("features")
      .setK(5)
      .setOutputCol("features")
  }
}
