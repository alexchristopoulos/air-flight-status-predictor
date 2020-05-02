package gr.upatras.ceid.ddcdm.predictor.prediction

import org.apache.spark.ml.regression.RandomForestRegressor

object RFPrediction {

  val rfRegression = new RandomForestRegressor()
    .setLabelCol("ARR_DELAY")

}
