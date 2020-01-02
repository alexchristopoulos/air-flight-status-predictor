package gr.upatras.ceid.ddcdm;

import gr.upatras.ceid.ddcdm.predictor.AirFlightStatusPredictor;

object app {

  def main(args: Array[String]): Unit = {

    var airFlightStatusPredictor:AirFlightStatusPredictor = new AirFlightStatusPredictor();

    airFlightStatusPredictor.run();

    println("Hello World!");
  }

}