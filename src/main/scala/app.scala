package gr.upatras.ceid.ddcdm;

import gr.upatras.ceid.ddcdm.predictor.AirFlightStatusPredictor
import gr.upatras.ceid.ddcdm.scrapy.tripAdvAirlineCarriersReviews
import gr.upatras.ceid.ddcdm.scrapy.sktrxAirportsReviews
object app {

  def main(args: Array[String]): Unit = {

    //var airFlightStatusPredictor:AirFlightStatusPredictor = new AirFlightStatusPredictor();

    //tripAdvAirlineCarriersReviews.extractReviews()
    //airFlightStatusPredictor.run();
sktrxAirportsReviews.execute()
    println("*******************END*******************");
  }

}