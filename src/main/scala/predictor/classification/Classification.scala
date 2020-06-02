package gr.upatras.ceid.ddcdm.predictor.classification

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.ml.classification.{GBTClassifier, MultilayerPerceptronClassifier, NaiveBayes, RandomForestClassifier, LinearSVC}
import gr.upatras.ceid.ddcdm.predictor.datasets.{TestDataset, TrainDataset}
import gr.upatras.ceid.ddcdm.predictor.config.config
import gr.upatras.ceid.ddcdm.predictor.util.MLUtils
import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, Dataset}

object Classification {

  private val sparkSession = gr.upatras.ceid.ddcdm.predictor.spark.Spark.getSparkSession()
  private var dimReductionMethod: PipelineStage = null
  var withPCA: Boolean = false

  val trainAndOrTest = (trainAndTest: Boolean, saveModel: Boolean, classifier: PipelineStage) => {

    var classifierCast: PipelineStage = null
    var classifierName: String = ""
    var resultsDir: String = config.sparkOutputDir
    var modelDir: String = config.sparkOutputDir

    println(s"WITH PCA: ${this.withPCA}")

    if (classifier.isInstanceOf[RandomForestClassifier]) {

      classifierName = "***RANDOM FOREST CLASSIFIER***"
      classifierCast = classifier.asInstanceOf[RandomForestClassifier]
      resultsDir = resultsDir + config.rfModelResults
      modelDir = modelDir + config.rfModelFolder

      println(s"Num trees: ${classifierCast.getParam("numTrees").toString()}")
      println(s"Max depth: ${classifierCast.getParam("maxDepth").toString()}")
      println(s"Num trees: ${classifierCast.getParam("numTrees").toString()}")
      println(s"Num trees: ${classifierCast.getParam("minInstancesPerNode").toString()}")

    } else if (classifier.isInstanceOf[NaiveBayes]) {

      classifierName = "***NAIVE BAYES***"
      classifierCast = classifier.asInstanceOf[NaiveBayes]
      resultsDir = resultsDir + config.naiveBayesResults
      modelDir = modelDir + config.naiveBayesFolder

    } else if (classifier.isInstanceOf[GBTClassifier]) {

      classifierName = "***GRADIENT BOOSTED TREE CLASSIFIER***"
      classifierCast = classifier.asInstanceOf[GBTClassifier]
      resultsDir = resultsDir + config.gbtcModelResults
      modelDir = modelDir + config.gbtcModelFolder

    } else if (classifier.isInstanceOf[MultilayerPerceptronClassifier]) {

      classifierName = "***MULTILAYER PERCEPTON***"
      classifierCast = classifier.asInstanceOf[MultilayerPerceptronClassifier]
      resultsDir = resultsDir + config.multilayerPerceptonResults
      modelDir = modelDir + config.multilayerPerceptonFolder

    } else if (classifier.isInstanceOf[LinearSVC]) {

      classifierName = "***Linear SVC***"
      classifierCast = classifier.asInstanceOf[LinearSVC]
      resultsDir = resultsDir + config.LinearSVCResults
      modelDir = modelDir + config.LinearSVCModel

    } else {

      throw new Exception("Invalid classification configuration!")
    }

    println(s"${classifierName} LOADING TRAIN DATASET")

    if (withPCA)
      TrainDataset.loadPCADataset()
    else
      TrainDataset.load()

    val delays = sparkSession.sql("SELECT * FROM TRAIN_FLIGHTS_DATA WHERE CANCELLED=1.0")
    val noDelays = sparkSession.sql("SELECT * FROM TRAIN_FLIGHTS_DATA WHERE CANCELLED=0.0 LIMIT " + (1.0 * delays.count().toDouble).toInt.toString())

    if (trainAndTest) {
      /* TRAIN AND TEST MODEL */

      println(s"${classifierName} {(> TRAINING MODEL AND TEST <)} ")

      val Array(noDelaysTrainSet, noDelaysTestSet) = noDelays.randomSplit(Array(0.7, 0.3), 7464681154782325311L)
      val Array(trainDelaysSet, testDelaysSet) = delays.randomSplit(Array(0.7, 0.3), 978951451735623534L)

      val pipelineTrainData = trainDelaysSet.union(noDelaysTrainSet)
      val pipelineTestData = testDelaysSet.union(noDelaysTestSet)


      var pipeline: Pipeline = null

      if (withPCA)
        pipeline = new Pipeline().setStages(
          Array(MLUtils.getVectorAssember(TrainDataset.getInputColsPCA(), "features"),
            classifierCast
          ))
      else
        pipeline = new Pipeline().setStages(
          Array(MLUtils.getVectorAssember(TrainDataset.getClassificationInputCols(), "features"),
            classifierCast
          ))


      println(s"${classifierName} TRAINING MODEL")
      val model = pipeline.fit(pipelineTrainData)

      if (saveModel) {

        model.write.overwrite().save(modelDir)
        println(s"${classifierName} SAVED MODEL")
      }

      println(s"${classifierName} TRAINED MODEL")
      println(s"${classifierName} TESTING MODEL")

      val predictions = model.transform(pipelineTestData)
      this.outputResultsMetrics(predictions, classifierName, resultsDir)

    } else {
      /* TRAIN ONLY MODEL */

      println(s"TRAIN AND SAVE MODEL ${classifierName}")

      val trainDataset = delays.union(noDelays)

      trainDataset.cache()

      var pipeline: Pipeline = null

      if (withPCA)
        pipeline = new Pipeline().setStages(
          Array(MLUtils.getVectorAssember(TrainDataset.getInputColsPCA(), "features"),
            classifierCast
          ))
      else
        pipeline = new Pipeline().setStages(
          Array(MLUtils.getVectorAssember(TrainDataset.getClassificationInputCols(), "features"),
            classifierCast
          ))


      println(s"${classifierName} TRAINING MODEL")
      val model = pipeline.fit(trainDataset)

      if (saveModel) {

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

    if (classifier.isInstanceOf[RandomForestClassifier]) {

      classifierName = "***RANDOM FOREST CLASSIFIER***"
      classifierCast = classifier.asInstanceOf[RandomForestClassifier]
      resultsDir = resultsDir + config.rfModelResults
      modelDir = modelDir + config.rfModelFolder


    } else if (classifier.isInstanceOf[NaiveBayes]) {

      classifierName = "***NAIVE BAYES***"
      classifierCast = classifier.asInstanceOf[NaiveBayes]
      resultsDir = resultsDir + config.naiveBayesResults
      modelDir = modelDir + config.naiveBayesFolder

    } else if (classifier.isInstanceOf[GBTClassifier]) {

      classifierName = "***GRADIENT BOOSTED TREE CLASSIFIER***"
      classifierCast = classifier.asInstanceOf[GBTClassifier]
      resultsDir = resultsDir + config.gbtcModelResults
      modelDir = modelDir + config.gbtcModelFolder

    } else if (classifier.isInstanceOf[MultilayerPerceptronClassifier]) {

      classifierName = "***MULTILAYER PERCEPTON***"
      classifierCast = classifier.asInstanceOf[MultilayerPerceptronClassifier]
      resultsDir = resultsDir + config.multilayerPerceptonResults
      modelDir = modelDir + config.multilayerPerceptonFolder

    } else if (classifier.isInstanceOf[LinearSVC]) {

      classifierName = "***Linear SVC***"
      classifierCast = classifier.asInstanceOf[LinearSVC]
      resultsDir = resultsDir + config.LinearSVCResults
      modelDir = modelDir + config.LinearSVCModel

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

  val setDimensionalityReductionMethod = (method: PipelineStage) => {
    this.dimReductionMethod = method
  }


  private val outputResultsMetrics = (predictions: DataFrame,
		classifierName: String,
		resultsDir: String) => {
		
    println(s"${classifierName} CALCULATING METRICS")

    val evaluator = MLUtils.getClassificationMultiClassEvaluator()
    val accuracy = evaluator.evaluate(predictions)

    println(s"${classifierName} CALCULATED ACCURACY")

    //RDD[prediction, label]
    val rddEval: org.apache.spark.rdd.RDD[(Double, Double)] = predictions
		.select("CANCELLED", "prediction")
		.rdd.map(row => ( row(1).toString().toDouble, row(0).toString().toDouble ))

    val metrics = new MulticlassMetrics(rddEval)
    val bw = new BufferedWriter(new FileWriter(resultsDir))

    bw.write("LABEL  PRECISION   RECALL  F-MEASURE")
    bw.newLine()
    println("LABEL  PRECISION   RECALL  F-MEASURE")

    metrics.labels.foreach(label => {

      bw.write(label.toString() + 
		  "  " + metrics.precision(label).toString() + 
		  "  " + metrics.recall(label).toString() + 
		  "   " + metrics.fMeasure(label).toString()
	  )
	  
      bw.newLine()
	  
      println(label.toString() + 
		"  " + metrics.precision(label).toString() + 
		"  " + metrics.recall(label).toString() + 
		"   " + metrics.fMeasure(label).toString()
	  )
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