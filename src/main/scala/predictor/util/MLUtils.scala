package gr.upatras.ceid.ddcdm.predictor.util

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler

object MLUtils {

  def getVectorAssember(inputCols: Array[String], featureCol: String): VectorAssembler = {
    new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol(featureCol)
  }

  def getClassificationMultiClassEvaluator(): MulticlassClassificationEvaluator = {
    return new MulticlassClassificationEvaluator()
      .setLabelCol("CANCELLED")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
  }

}
