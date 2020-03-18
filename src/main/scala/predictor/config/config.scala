package gr.upatras.ceid.ddcdm.predictor.config;

object config {

  val sparkConfSetMaster = "local[*]"
  val sparkConfSetAppName = "AIR-FLIGHT-STATUS-PREDICTOR"
  val sparkDatasetDir = "/home/admin/data"
  val sparkDatasetAirportsSkytraxReviews = "/dataAirportsSkytraxReviews.out"
  val sparkDatasetAirportsAirlinesQualityReviews = "/dataAirportsAirlinequality.out"
  val sparkDatasetIataAirportCodesWiki = "/iataAirportsCodesWiki.out"
  val sparkDatasetTripadvisorAirlinesReviews = "/tripadvisorreviews.data"
  val sparkDatasetPredictionAirports = "/airports.csv"
  val sparkDatasetPredictionAirlines = "/airlines.csv"
  val sparkDatasetPredictionFlights = "/subset.data"
  val sparkOutputDataset = "/home/admin/data/output/"
  val sparkAirFlightsKaggleDataset2015 = "/flights.csv"

}