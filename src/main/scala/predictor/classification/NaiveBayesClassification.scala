package gr.upatras.ceid.ddcdm.predictor.classification

import java.io.{BufferedWriter, FileWriter}

import gr.upatras.ceid.ddcdm.predictor.datasets.{TestDataset, TrainDataset}
import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object NaiveBayesClassification {

  private val sparkSession = Spark.getSparkSession()
  private var model: PipelineModel = _
  private var isLoaded:Boolean = false

  def loadExistingModel(): Unit = {

    println("Loading NAIVE BAYES MODEL")
    this.model = PipelineModel.load("/home/admin/naiveBayesModel")
    println("lOADING  NAIVE BAYES MODEL LOADED")
    this.isLoaded = true
  }

  def trainModel(trainAndTest: Boolean, saveModel: Boolean): Unit = {

    TrainDataset.load()

    if(trainAndTest){

      val delays = sparkSession.sql("SELECT * FROM TRAIN_FLIGHTS_DATA WHERE CANCELLED=1.0")
      val noDelays = sparkSession.sql("SELECT * FROM TRAIN_FLIGHTS_DATA WHERE CANCELLED=0.0 LIMIT " + (1.00 * delays.count().toDouble).toInt.toString())

      val vectorAssembler = new VectorAssembler()
        .setInputCols(TrainDataset.getClassificationInputCols())
        .setOutputCol("features")

      val Array(noDelaysTrainSet, noDelaysTestSet) =   noDelays.randomSplit(Array(0.65, 0.35), 11L)
      val Array(trainDelaysSet, testDelaysSet) =   delays.randomSplit(Array(0.65, 0.35), 11L)

      val pipelineTrainData = trainDelaysSet.union(noDelaysTrainSet)
      val pipelineTestData = testDelaysSet.union(noDelaysTestSet)

      pipelineTrainData.cache()

      val naiveBayes = new NaiveBayes()
        .setLabelCol("CANCELLED")

      val stages = Array(vectorAssembler, naiveBayes)
      val pipeline = new Pipeline().setStages(stages)

      println("Training Model")

      val model = pipeline.fit(pipelineTrainData)

      println("Trained Model")

      val predictions = model.transform(pipelineTestData)

      println("Testing model")

      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("CANCELLED")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")

      val accuracy = evaluator
        .evaluate(predictions)

      println("CALCULATED ACCURACY")

      if(saveModel) {

        model
          .write
          .overwrite()
          .save("/home/admin/naiveBayesModel")
      }

      //RDD[prediction, label]
      val rddEval: org.apache.spark.rdd.RDD[(Double, Double)] = predictions.select("CANCELLED", "prediction").rdd.map(row => ( row(1).toString().toDouble, row(0).toString().toDouble ))

      val metrics = new MulticlassMetrics(rddEval)
      val bw = new BufferedWriter(new FileWriter("/home/admin/naive_bayes_results.txt"))

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

    } else {

      val delays = sparkSession.sql("SELECT * FROM TRAIN_FLIGHTS_DATA WHERE CANCELLED=1.0")
      val noDelays = sparkSession.sql("SELECT * FROM TRAIN_FLIGHTS_DATA WHERE CANCELLED=0.0 LIMIT " + delays.count().toString())

      val trainDataset = delays.union(noDelays)

      val vectorAssembler = new VectorAssembler()
        .setInputCols(TrainDataset.getClassificationInputCols())
        .setOutputCol("features")

      val naiveBayes = new NaiveBayes()
        .setLabelCol("CANCELLED")

      val stages = Array(vectorAssembler, naiveBayes)
      val pipeline = new Pipeline().setStages(stages)

      println("TRAINING NAIVE BAYES MODEL")

      this.model = pipeline.fit(trainDataset)

      println("SAVING NAIVE BAYES MODEL")

      if(saveModel)

        model
          .write
          .overwrite()
          .save("/home/admin/naiveBayesModel")

      println("**NAIVE BAYES MODEL TRAINED AND SAVED***")
    }
  }

  def predict(viewName: String): Unit = {


    println("***NAIVE BAYES MODEL CLASSIFICATION PREDICTION***")
    this.loadExistingModel()
    println("MODEL LOADED")

    TestDataset.load()
    val testData = TestDataset.getDataFrame()

    val predictions = this.model.transform(testData)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("CANCELLED")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)

    //RDD[prediction, label]
    val rddEval: org.apache.spark.rdd.RDD[(Double, Double)] = predictions.select("CANCELLED", "prediction").rdd.map(row => ( row(1).toString().toDouble, row(0).toString().toDouble ))

    val metrics = new MulticlassMetrics(rddEval)
    val bw = new BufferedWriter(new FileWriter("/home/admin/naive_bayes_results.txt"))

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

  def getMetrics(): Unit = {

  }


}
