package gr.upatras.ceid.ddcdm.predictor.config;

object config {

  val sparkConfSetMaster = "local[*]"
  val sparkConfSetAppName = "AIR-FLIGHT-STATUS-PREDICTOR"

  val sparkDatasetDir = "/home/admin/data"
  val sparkDataResources = "/home/admin/deploy/src/main/resources"
  val sparkOutputDir = "/home/admin"

  val sparkAirlines = "/airlines.csv"
  val sparkAirports = "/airports.csv"
  val sparkTrainDataset = "/flights2017-2018.csv"
  val sparkTestDataset = "/flights2019.csv"
  val sparkAirlineReviewsDataset = "/airlineReviews.csv"
  val sparkAirlinesAVGDel = "/airlinesAVGDelays.csv"
  val sparkAirportsAvgDepDelay = "/airportsAVGDepDelay.csv"

  val rfModelFolder = "/randomForestModel"
  val gbtcModelFolder = "/gbtcModel"
  val multilayerPerceptonFolder = "/multiLayerPerceptonModel"
  val naiveBayesFolder = "/naiveBayesModel2"
  val rfModelResults = "/randomForestResults.txt"
  val gbtcModelResults = "/gbtcResults.txt"
  val multilayerPerceptonResults = "/multiLayerPerceptonResults.txt"
  val naiveBayesResults = "/naiveBayesResults2.txt"



















  /*
  OLD CONFIG
   */
  val sparkDatasetAirportsSkytraxReviews = "/dataAirportsSkytraxReviews.out"
  val sparkDatasetAirportsAirlinesQualityReviews = "/dataAirportsAirlinequality.out"
  val sparkDatasetIataAirportCodesWiki = "/iataAirportsCodesWiki.out"
  val sparkDatasetTripadvisorAirlinesReviews = "/tripadvisorreviews.data"
  val sparkDatasetPredictionAirports = "/kaggleairports.data"
  val sparkDatasetPredictionAirlines = "/airlines.csv"
  val sparkDatasetPredictionFlights = "/kaggleflights.data"
  val sparkOutputDataset = "/home/admin/data/output/"
  val sparkAirFlightsKaggleDataset2015 = "/flights2017-2018.csv"


}