package gr.upatras.ceid.ddcdm

import gr.upatras.ceid.ddcdm.predictor.AirFlightStatusPredictor
import gr.upatras.ceid.ddcdm.scrapy.tripAdvAirlineCarriersReviews
//import gr.upatras.ceid.ddcdm.scrapy.sktrxAirportsReviews

object App {

  def main(args: Array[String]): Unit = {

    println("*******************AIR-FLIGHT-STATUS-PREDICTOR*******************")
    tripAdvAirlineCarriersReviews.extractReviews()
   // AirFlightStatusPredictor.run()
    println("*******************END*******************");
  }


  private def scrappyRun(): Unit = {
    return
        /*
        * ********************* CODE USED FOR SCRAPPYING *****************
        * tripAdvAirlineCarriersReviews.extractReviews()
        * sktrxAirportsReviews.execute()
        *
        * */
  }

}