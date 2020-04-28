package gr.upatras.ceid.ddcdm.predictor.util

import org.apache.spark.ml.feature.VectorAssembler

object MLUtils {

  def getVectorAssember(inputCols: Array[String], featureCol: String): VectorAssembler = {
    new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol(featureCol)
  }
}
