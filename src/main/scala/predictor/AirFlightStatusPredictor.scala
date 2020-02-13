package gr.upatras.ceid.ddcdm.predictor

import gr.upatras.ceid.ddcdm.predictor.spark.Spark;

class AirFlightStatusPredictor {

  def AirFlightStatusPredictor(): Unit = {}

  def run(): Unit = {

    var spark: Spark = new Spark();

    spark.test();
    spark.exit();
  }

}