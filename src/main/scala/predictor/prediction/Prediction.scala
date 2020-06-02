package gr.upatras.ceid.ddcdm.predictor.prediction

import java.io.{BufferedWriter, FileWriter}

import gr.upatras.ceid.ddcdm.predictor.config.config
import gr.upatras.ceid.ddcdm.predictor.datasets.{TestDataset, TrainDataset}
import gr.upatras.ceid.ddcdm.predictor.util.MLUtils
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.ml.regression.{IsotonicRegression, LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.DataFrame

object Prediction {

  private val sparkSession = gr.upatras.ceid.ddcdm.predictor.spark.Spark.getSparkSession()
  private var dimReductionMethod: PipelineStage = null

  val trainAndOrTest = (trainAndTest: Boolean, saveModel: Boolean, predictor: PipelineStage) => {

    var predictorCast: PipelineStage = null
    var predictorName: String = ""
    var resultsDir: String = config.sparkOutputDir
    var modelDir: String = config.sparkOutputDir

    if (predictor.isInstanceOf[LinearRegression]) {

      predictorName = "***LINEAR REGRESSION***"
      predictorCast = predictor.asInstanceOf[LinearRegression]
      resultsDir = resultsDir + config.linearRegressorResults
      modelDir = modelDir + config.linearRegressorFolder

    } else if (predictor.isInstanceOf[IsotonicRegression]) {

      predictorName = "***ISOTONIC REGRESSION***"
      predictorCast = predictor.asInstanceOf[IsotonicRegression]
      resultsDir = resultsDir + config.isotonicRegressorResults
      modelDir = modelDir + config.isotonicRegressorFolder

    } else if (predictor.isInstanceOf[RandomForestRegressor]) {

      predictorName = "***RANDOM FOREST REGRESSION***"
      predictorCast = predictor.asInstanceOf[RandomForestRegressor]
      resultsDir = resultsDir + config.rfRegressionModelResults
      modelDir = modelDir + config.rfRegressionModelFolder

    } else {

      throw new Exception("Invalid regression configuration!")
    }

    println(s"${predictorName} LOADING TRAIN DATASET")

    TrainDataset.load()

    val flights = sparkSession.sql("SELECT * FROM TRAIN_FLIGHTS_DATA WHERE ARR_DELAY IS NOT NULL")

    if (trainAndTest) {
      /* TRAIN AND TEST MODEL */

      val Array(pipelineTrainData, pipelineTestData) = flights.randomSplit(Array(0.7, 0.3), 11L)

      val pipeline = new Pipeline().setStages(
        Array(MLUtils.getVectorAssember(TrainDataset.getRegressionInputCols(), "features"),
          predictorCast
        ))

      println(s"${predictorName} TRAINING MODEL")
      val model = pipeline.fit(pipelineTrainData)

      if (saveModel) {

        model.write.overwrite().save(modelDir)
        println(s"${predictorName} SAVED MODEL")
      }

      println(s"${predictorName} TRAINED MODEL")
      println(s"${predictorName} TESTING MODEL")

      val predictions = model.transform(pipelineTestData)

      this.outputResultsMetrics(predictions, predictorName, resultsDir)

    } else {
      /* TRAIN ONLY MODEL */

      val trainDataset = flights

      trainDataset.cache()

      val pipeline = new Pipeline().setStages(
        Array(MLUtils.getVectorAssember(TrainDataset.getRegressionInputCols(), "features"),
          predictorCast
        ))

      println(s"${predictorName} TRAINING MODEL")
      val model = pipeline.fit(trainDataset)

      if (saveModel) {

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

    if (predictor.isInstanceOf[LinearRegression]) {

      predictorName = "***LINEAR REGRESSION***"
      predictorCast = predictor.asInstanceOf[LinearRegression]
      resultsDir = resultsDir + config.linearRegressorResults
      modelDir = modelDir + config.linearRegressorFolder

    } else if (predictor.isInstanceOf[IsotonicRegression]) {

      predictorName = "***ISOTONIC REGRESSION***"
      predictorCast = predictor.asInstanceOf[IsotonicRegression]
      resultsDir = resultsDir + config.isotonicRegressorResults
      modelDir = modelDir + config.isotonicRegressorFolder

    } else if (predictor.isInstanceOf[RandomForestRegressor]) {

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

    val testData = sparkSession.sql("SELECT tf.*, tf.ARR_DELAY AS label FROM TEST_FLIGHTS_DATA AS tf WHERE ARR_DELAY IS NOT NULL")
    val predictions = model.transform(testData)

    this.outputResultsMetrics(predictions, predictorName, resultsDir)
  }

  val setDimensionalityReductionMethod = (method: PipelineStage) => {
    this.dimReductionMethod = method
  }

  //EVALUATION FUNCTION THAT WRITES THE RESULTS
  private val outputResultsMetrics = (predictions: DataFrame,
	regressorName: String,
	resultsDir: String) => {

    println(s"${regressorName} CALCULATING METRICS")

    val evaluator = new RegressionEvaluator()
      .setLabelCol("ARR_DELAY")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    println(s"${regressorName} CALCULATED RMSE")
    val bw = new BufferedWriter(new FileWriter(resultsDir))
    bw.write(s"${regressorName} RMSE = ${rmse.toString()}")
    bw.newLine()
    bw.write("*****************************************")
    println(s"${regressorName} RMSE = ${rmse.toString()}")
    bw.close()

  }

  //DUMENSIONALITY REDUCTION
  private val reduceDimenionsDf: DataFrame => DataFrame = (dataset: DataFrame) => {

    if (dimReductionMethod == null)
      dataset
    else if (dimReductionMethod.isInstanceOf[PCA])

      dimReductionMethod
        .asInstanceOf[PCA]
        .fit(dataset)
        .transform(dataset)
    else
      dataset
  }
}
