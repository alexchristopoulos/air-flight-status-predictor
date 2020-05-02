package gr.upatras.ceid.ddcdm.predictor.prediction

import org.apache.spark.ml.regression.IsotonicRegression

object IsotonicPrediction {

  val isotonicRegression = new IsotonicRegression()
    .setLabelCol("ARR_DELAY")

}
