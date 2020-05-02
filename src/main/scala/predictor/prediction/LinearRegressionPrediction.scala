package gr.upatras.ceid.ddcdm.predictor.prediction

import org.apache.spark.ml.regression.LinearRegression

object LinearRegressionPrediction {

  val linearRegression = new LinearRegression()
    .setLabelCol("ARR_DELAY")
}
