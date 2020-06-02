package gr.upatras.ceid.ddcdm.predictor.prediction

import org.apache.spark.ml.regression.RandomForestRegressor

object RFPrediction {

  val rfRegression = new RandomForestRegressor()
    .setLabelCol("ARR_DELAY")
    .setNumTrees(65)
    .setSubsamplingRate(0.9)
    .setMaxDepth(12)
    .setMinInstancesPerNode(3)

}
