package gr.upatras.ceid.ddcdm.predictor.preprocess

import org.apache.spark.ml.feature.PCA

object PrincipalComponentAnalysis {


  def getPca(inputCol: String, K: Int, outputCol: String): PCA = {

     new PCA()
      .setInputCol(inputCol)
      .setK(K)
      .setOutputCol(outputCol)
  }
}
