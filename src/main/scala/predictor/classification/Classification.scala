package gr.upatras.ceid.ddcdm.predictor.classification

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.{GBTClassifier, MultilayerPerceptronClassifier, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import gr.upatras.ceid.ddcdm.predictor.datasets.{TestDataset, TrainDataset}
import gr.upatras.ceid.ddcdm.predictor.config.config
import gr.upatras.ceid.ddcdm.predictor.util.MLUtils
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame

object Classification {

  private val sparkSession = gr.upatras.ceid.ddcdm.predictor.spark.Spark.getSparkSession()

  val trainAndOrTest = (trainAndTest: Boolean, saveModel: Boolean, classifier: PipelineStage) => {

    var classifierCast: PipelineStage = null
    var classifierName: String = ""
    var resultsDir: String = config.sparkOutputDir
    var modelDir: String = config.sparkOutputDir

    if(classifier.isInstanceOf[RandomForestClassifier]){

      classifierName = "***RANDOM FOREST CLASSIFIER***"
      classifierCast = classifier.asInstanceOf[RandomForestClassifier]
      resultsDir = resultsDir + config.rfModelResults
      modelDir = modelDir + config.rfModelFolder

    } else if(classifier.isInstanceOf[NaiveBayes]) {

      classifierName = "***NAIVE BAYES***"
      classifierCast = classifier.asInstanceOf[NaiveBayes]
      resultsDir = resultsDir + config.naiveBayesResults
      modelDir = modelDir + config.naiveBayesFolder

    } else if(classifier.isInstanceOf[GBTClassifier]) {

      classifierName = "***GRADIENT BOOSTED TREE CLASSIFIER***"
      classifierCast = classifier.asInstanceOf[GBTClassifier]
      resultsDir = resultsDir + config.gbtcModelResults
      modelDir = modelDir + config.gbtcModelFolder

    } else if(classifier.isInstanceOf[MultilayerPerceptronClassifier]) {

      classifierName = "***MULTILAYER PERCEPTON***"
      classifierCast = classifier.asInstanceOf[MultilayerPerceptronClassifier]
      resultsDir = resultsDir + config.multilayerPerceptonResults
      modelDir = modelDir + config.multilayerPerceptonFolder

    } else {

      throw new Exception("Invalid classification configuration!")
    }

    println(s"${classifierName} LOADING TRAIN DATASET")
    TrainDataset.load()

    if(trainAndTest){  /* TRAIN AND TEST MODEL */

      val delays = sparkSession.sql("SELECT * FROM TRAIN_FLIGHTS_DATA WHERE CANCELLED=1.0")
      val noDelays = sparkSession.sql("SELECT * FROM TRAIN_FLIGHTS_DATA WHERE CANCELLED=0.0 LIMIT " + (1.00 * delays.count().toDouble).toInt.toString())

      val Array(noDelaysTrainSet, noDelaysTestSet) =   noDelays.randomSplit(Array(0.65, 0.35), 11L)
      val Array(trainDelaysSet, testDelaysSet) =   delays.randomSplit(Array(0.65, 0.35), 11L)

      val pipelineTrainData = trainDelaysSet.union(noDelaysTrainSet)
      val pipelineTestData = testDelaysSet.union(noDelaysTestSet)

      val pipeline = new Pipeline().setStages(
        Array(MLUtils.getVectorAssember(TrainDataset.getClassificationInputCols(), "features"),
        classifierCast
        ))

      println(s"${classifierName} TRAINING MODEL")
      val model = pipeline.fit(pipelineTrainData)

      if(saveModel) {

        model.write.overwrite().save(modelDir)
        println(s"${classifierName} SAVED MODEL")
      }

      println(s"${classifierName} TRAINED MODEL")
      println(s"${classifierName} TESTING MODEL")

      val predictions = model.transform(pipelineTestData)
      this.outputResultsMetrics(predictions, classifierName, resultsDir)

    } else {    /* TRAIN ONLY MODEL */

      val delays = sparkSession.sql("SELECT * FROM TRAIN_FLIGHTS_DATA WHERE CANCELLED=1.0")
      val noDelays = sparkSession.sql("SELECT * FROM TRAIN_FLIGHTS_DATA WHERE CANCELLED=0.0 LIMIT " + delays.count().toString())

      val trainDataset = delays.union(noDelays)

      val pipeline = new Pipeline().setStages(
        Array(MLUtils.getVectorAssember(TrainDataset.getClassificationInputCols(), "features"),
          classifierCast
        ))

      println(s"${classifierName} TRAINING MODEL")
      val model = pipeline.fit(trainDataset)

      if(saveModel) {

        model.write.overwrite().save(modelDir)
        println(s"${classifierName} SAVED MODEL")
      }

      println("TRAINED MODEL")
    }
  }

  //LOAD MODEL AND CLASSIFY TEST DATA
  val classify = (classifier: PipelineStage) => {

    var classifierCast: PipelineStage = null
    var classifierName: String = ""
    var resultsDir: String = config.sparkOutputDir
    var modelDir: String = config.sparkOutputDir

    if(classifier.isInstanceOf[RandomForestClassifier]){

      classifierName = "***RANDOM FOREST CLASSIFIER***"
      classifierCast = classifier.asInstanceOf[RandomForestClassifier]
      resultsDir = resultsDir + config.rfModelResults
      modelDir = modelDir + config.rfModelFolder

    } else if(classifier.isInstanceOf[NaiveBayes]) {

      classifierName = "***NAIVE BAYES***"
      classifierCast = classifier.asInstanceOf[NaiveBayes]
      resultsDir = resultsDir + config.naiveBayesResults
      modelDir = modelDir + config.naiveBayesFolder

    } else if(classifier.isInstanceOf[GBTClassifier]) {

      classifierName = "***GRADIENT BOOSTED TREE CLASSIFIER***"
      classifierCast = classifier.asInstanceOf[GBTClassifier]
      resultsDir = resultsDir + config.gbtcModelResults
      modelDir = modelDir + config.gbtcModelFolder

    } else if(classifier.isInstanceOf[MultilayerPerceptronClassifier]) {

      classifierName = "***MULTILAYER PERCEPTON***"
      classifierCast = classifier.asInstanceOf[MultilayerPerceptronClassifier]
      resultsDir = resultsDir + config.multilayerPerceptonResults
      modelDir = modelDir + config.multilayerPerceptonFolder

    } else {

      throw new Exception("Invalid classification configuration!")
    }

    println(s"${classifierName} LOADING MODEL")
    val model = PipelineModel.load(modelDir)

    println(s"${classifierName} LOADING TEST DATASET")
    TestDataset.load()

    val testData = TestDataset.getDataFrame()
    val predictions = model.transform(testData)

    this.outputResultsMetrics(predictions, classifierName, resultsDir)
  }

  //EVALUATION FUNCTION THAT WRITES THE RESULTS
  private val outputResultsMetrics = (predictions: DataFrame, classifierName: String, resultsDir: String) => {

    println(s"${classifierName} CALCULATING METRICS")

    val evaluator = MLUtils.getClassificationMultiClassEvaluator()
    val accuracy = evaluator.evaluate(predictions)

    println(s"${classifierName} CALCULATED ACCURACY")

    //RDD[prediction, label]
    val rddEval: org.apache.spark.rdd.RDD[(Double, Double)] = predictions.select("CANCELLED", "prediction").rdd.map(row => ( row(1).toString().toDouble, row(0).toString().toDouble ))

    val metrics = new MulticlassMetrics(rddEval)
    val bw = new BufferedWriter(new FileWriter(resultsDir))

    bw.write("LABEL  PRECISION   RECALL  F-MEASURE")
    bw.newLine()
    println("LABEL  PRECISION   RECALL  F-MEASURE")

    metrics.labels.foreach(label => {

      bw.write(label.toString() + "  " + metrics.precision(label).toString() + "  " + metrics.recall(label).toString() + "   " + metrics.fMeasure(label).toString())
      bw.newLine()
      println(label.toString() + "  " + metrics.precision(label).toString() + "  " + metrics.recall(label).toString() + "   " + metrics.fMeasure(label).toString())
    })

    println("\n\nWEIGHTED_PRECISION   WEIGHTED_RECALL   WEIGHTED_F1")
    bw.write("\n\nWEIGHTED_PRECISION   WEIGHTED_RECALL   WEIGHTED_F1")
    bw.newLine()
    bw.write(metrics.weightedPrecision + "   " + metrics.weightedRecall + "   " + metrics.weightedFMeasure + "\n")
    println(metrics.weightedPrecision + "   " + metrics.weightedRecall + "   " + metrics.weightedFMeasure + "\n")
    bw.newLine()

    bw.write(s"Test set accuracy = $accuracy")
    println(s"Test set accuracy = $accuracy")
    bw.close()

  }

}