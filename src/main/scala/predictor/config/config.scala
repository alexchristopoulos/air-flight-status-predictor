package gr.upatras.ceid.ddcdm.predictor.config;

object config {

  val sparkConfSetMaster = "local[*]"
  val sparkConfSetAppName = "AIR-FLIGHT-STATUS-PREDICTOR"

  //FOLDERS
  val sparkDatasetDir = "/home/admin/data"
  val sparkDataResources = "/home/admin/deploy/src/main/resources"
  val sparkOutputDir = "/home/admin"

  //DATASETS
  val sparkTrainDataset = "/2017-2019flights.csv"
  //val sparkTrainDataset = "/flights2017-2018.csv"
  val sparkTestDataset = "/flights2019.csv"

  //RESOURCE FILES
  val sparkAirlines = "/airlines.csv"
  val sparkAirports = "/airports.csv"
  val sparkAirlineReviewsDataset = "/airlineReviews.csv"
  val sparkAirlinesAVGDel = "/airlinesAVGDelays.csv"
  val sparkAirportsAvgDepDelay = "/airportsAVGDepDelay.csv"

 //MODEL DIR NAMES CLASSIFICATION
  val rfModelFolder = "/randomForestModel"
  val gbtcModelFolder = "/gbtcModel"
  val LinearSVCModel = "/lsvcModel"
  val multilayerPerceptonFolder = "/multiLayerPerceptonModel"
  val naiveBayesFolder = "/naiveBayesModel"

  //MODEL FILENAMES RESUTLS CLASSIFICATION
  val rfModelResults = "/randomForestResults.txt"
  val gbtcModelResults = "/gbtcClassificationResults.txt"
  val LinearSVCResults = "/lsvcRsults.txt"
  val multilayerPerceptonResults = "/multiLayerPerceptonResults.txt"
  val naiveBayesResults = "/naiveBayesResults.txt"


  //MODEL DIR NAMES REGRESSION
  val rfRegressionModelFolder = "/randomForestRegressionModel"
  val isotonicRegressorFolder = "/isotonicRegressionModel"
  val linearRegressorFolder = "/linearRegressionModel"

  //MODEL FILENAMES RESUTLS REGRESSION
  val rfRegressionModelResults = "/randomForestRegressionResults.txt"
  val isotonicRegressorResults = "/isotonicRegressionResults.txt"
  val linearRegressorResults = "/linearRegressorResults.txt"















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