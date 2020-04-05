package gr.upatras.ceid.ddcdm.predictor.config;

object config {

  val sparkConfSetMaster = "local[*]"
  val sparkConfSetAppName = "AIR-FLIGHT-STATUS-PREDICTOR"
  val sparkDatasetDir = "/home/admin/data"
  val sparkDatasetAirportsSkytraxReviews = "/dataAirportsSkytraxReviews.out"
  val sparkDatasetAirportsAirlinesQualityReviews = "/dataAirportsAirlinequality.out"
  val sparkDatasetIataAirportCodesWiki = "/iataAirportsCodesWiki.out"
  val sparkDatasetTripadvisorAirlinesReviews = "/tripadvisorreviews.data"
  val sparkDatasetPredictionAirports = "/kaggleairports.data"
  val sparkDatasetPredictionAirlines = "/airlines.csv"
  val sparkDatasetPredictionFlights = "/kaggleflights.data"
  val sparkOutputDataset = "/home/admin/data/output/"
  val sparkAirFlightsKaggleDataset2015 = "/flights.csv"


  val sparkAirlines = "/airlines.csv"
  val sparkAirports = "/airports.csv"
  val sparkTrainDataset = "/flights2019.csv"
  val sparkTestDataset = "flights2019.csv"
}