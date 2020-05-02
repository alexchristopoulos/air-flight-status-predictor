package gr.upatras.ceid.ddcdm.predictor.prediction

import gr.upatras.ceid.ddcdm.predictor.config.config
import gr.upatras.ceid.ddcdm.predictor.datasets.{TestDataset, TrainDataset}
import gr.upatras.ceid.ddcdm.predictor.util.MLUtils
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.regression.{LinearRegression, IsotonicRegression, RandomForestRegressor}

object Prediction {

  private val sparkSession = gr.upatras.ceid.ddcdm.predictor.spark.Spark.getSparkSession()

  val trainAndOrTest = (trainAndTest: Boolean, saveModel: Boolean, predictor: PipelineStage) => {

    var predictorCast: PipelineStage = null
    var predictorName: String = ""
    var resultsDir: String = config.sparkOutputDir
    var modelDir: String = config.sparkOutputDir

    if(predictor.isInstanceOf[LinearRegression]){

      predictorName = "***LINEAR REGRESSION***"
      predictorCast = predictor.asInstanceOf[LinearRegression]
      resultsDir = resultsDir + config.linearRegressorResults
      modelDir = modelDir + config.linearRegressorFolder

    } else if(predictor.isInstanceOf[IsotonicRegression]) {

      predictorName = "***ISOTONIC REGRESSION***"
      predictorCast = predictor.asInstanceOf[IsotonicRegression]
      resultsDir = resultsDir + config.isotonicRegressorResults
      modelDir = modelDir + config.isotonicRegressorFolder

    } else if(predictor.isInstanceOf[RandomForestRegressor]) {

      predictorName = "***RANDOM FOREST REGRESSION***"
      predictorCast = predictor.asInstanceOf[RandomForestRegressor]
      resultsDir = resultsDir + config.rfRegressionModelResults
      modelDir = modelDir + config.rfRegressionModelFolder

    } else {

      throw new Exception("Invalid regression configuration!")
    }

    println(s"${predictorName} LOADING TRAIN DATASET")

    TrainDataset.load()

    val delays = sparkSession.sql("SELECT * FROM TRAIN_FLIGHTS_DATA WHERE CANCELLED=1.0")
    val noDelays = sparkSession.sql("SELECT * FROM TRAIN_FLIGHTS_DATA WHERE CANCELLED=0.0 LIMIT " + delays.count().toInt.toString())

    if(trainAndTest){  /* TRAIN AND TEST MODEL */

      val Array(noDelaysTrainSet, noDelaysTestSet) =   noDelays.randomSplit(Array(0.65, 0.35), 11L)
      val Array(trainDelaysSet, testDelaysSet) =   delays.randomSplit(Array(0.65, 0.35), 11L)

      val pipelineTrainData = trainDelaysSet.union(noDelaysTrainSet)
      val pipelineTestData = testDelaysSet.union(noDelaysTestSet)

      val pipeline = new Pipeline().setStages(
        Array(MLUtils.getVectorAssember(TrainDataset.getClassificationInputCols(), "features"),
          predictorCast
        ))

      println(s"${predictorName} TRAINING MODEL")
      val model = pipeline.fit(pipelineTrainData)

      if(saveModel) {

        model.write.overwrite().save(modelDir)
        println(s"${predictorName} SAVED MODEL")
      }

      println(s"${predictorName} TRAINED MODEL")
      println(s"${predictorName} TESTING MODEL")

      val predictions = model.transform(pipelineTestData)

    } else {    /* TRAIN ONLY MODEL */

      val trainDataset = delays.union(noDelays)

      trainDataset.cache()

      val pipeline = new Pipeline().setStages(
        Array(MLUtils.getVectorAssember(TrainDataset.getClassificationInputCols(), "features"),
          predictorCast
        ))

      println(s"${predictorName} TRAINING MODEL")
      val model = pipeline.fit(trainDataset)

      if(saveModel) {

        model.write.overwrite().save(modelDir)
        println(s"${predictorName} SAVED MODEL")
      }

      println("TRAINED MODEL")
    }
  }

  //LOAD MODEL AND PREDICT
  val predict = (predictor: PipelineStage) => {

    var predictorCast: PipelineStage = null
    var predictorName: String = ""
    var resultsDir: String = config.sparkOutputDir
    var modelDir: String = config.sparkOutputDir

    if(predictor.isInstanceOf[LinearRegression]){

      predictorName = "***LINEAR REGRESSION***"
      predictorCast = predictor.asInstanceOf[LinearRegression]
      resultsDir = resultsDir + config.linearRegressorResults
      modelDir = modelDir + config.linearRegressorFolder

    } else if(predictor.isInstanceOf[IsotonicRegression]) {

      predictorName = "***ISOTONIC REGRESSION***"
      predictorCast = predictor.asInstanceOf[IsotonicRegression]
      resultsDir = resultsDir + config.isotonicRegressorResults
      modelDir = modelDir + config.isotonicRegressorFolder

    } else if(predictor.isInstanceOf[RandomForestRegressor]) {

      predictorName = "***RANDOM FOREST REGRESSION***"
      predictorCast = predictor.asInstanceOf[RandomForestRegressor]
      resultsDir = resultsDir + config.rfRegressionModelResults
      modelDir = modelDir + config.rfRegressionModelFolder

    } else {

      throw new Exception("Invalid regression configuration!")
    }

    println(s"${predictorName} LOADING MODEL")
    val model = PipelineModel.load(modelDir)

    println(s"${predictorName} LOADING TEST DATASET")
    TestDataset.load()

    val testData = TestDataset.getDataFrame()
    val predictions = model.transform(testData)

  }
}
