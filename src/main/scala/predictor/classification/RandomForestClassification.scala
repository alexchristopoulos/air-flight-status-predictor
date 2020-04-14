package gr.upatras.ceid.ddcdm.predictor.classification

import gr.upatras.ceid.ddcdm.predictor.datasets.{Airlines, Airports, TestDataset, TrainDataset}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{RFormula, StringIndexer, VectorAssembler}
import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import java.io._

object RandomForestClassification {

  private val sparkSession = Spark.getSparkSession()
  private var model: PipelineModel = _
  private var isLoaded:Boolean = false

  def loadExistingModel(): Unit = {

    println("Loading Random Forest Model")
    this.model = PipelineModel.load("/home/admin/randomForestModel")
    println("RANDOM FOREST MODEL LOADED")
    this.isLoaded = true
  }

  def trainModel(trainAndTest: Boolean, saveModel: Boolean): Unit = {

    Airports.load()
    Airlines.load()
    TrainDataset.load()

    sparkSession
      .sql("SELECT CONCAT(f.DAY_OF_MONTH, '-', f.MONTH, '-', f.YEAR, '/' ,a1.id, '-', a1.iata) AS DATE_ORIGIN_ID, " +
        "f.MONTH, f.DAY_OF_MONTH, f.DAY_OF_WEEK, l.id AS OP_CARRIER_ID, a1.id AS ORIGIN, a2.id AS DESTINATION, f.CANCELLED, f.DISTANCE " +
        "FROM TRAIN_FLIGHTS_DATA AS f " +
        "INNER JOIN airlines AS l ON f.OP_CARRIER_ID=l.iata " +
        "INNER JOIN airports AS a1 ON f.ORIGIN=a1.iata " +
        "INNER JOIN airports AS a2 ON f.DESTINATION=a2.iata " +
        "WHERE f.DIVERTED!=1.0")
      .as("FLIGHTS_DATA")
      .createOrReplaceTempView("FLIGHTS_DATA")

    sparkSession.sql("SELECT DATE_ORIGIN_ID, COUNT(*) AS FLIGHTS_COUNT FROM FLIGHTS_DATA AS ff GROUP BY ff.DATE_ORIGIN_ID").createOrReplaceTempView("NUM_OF_FLIGHTS_PER_DATE_PER_ORIGIN")

    sparkSession.sql("SELECT f.*, cf.FLIGHTS_COUNT AS FLIGHTS_COUNT FROM FLIGHTS_DATA AS f " +
      "INNER JOIN NUM_OF_FLIGHTS_PER_DATE_PER_ORIGIN AS cf ON f.DATE_ORIGIN_ID=cf.DATE_ORIGIN_ID")
      .as("FLIGHTS_DATA")
      .createOrReplaceTempView("FLIGHTS_DATA")

    if(trainAndTest){

      val delays = sparkSession.sql("SELECT * FROM FLIGHTS_DATA WHERE CANCELLED=1.0")
      val cancelations = sparkSession.sql("SELECT * FROM FLIGHTS_DATA WHERE CANCELLED=2.0")
      val noDelays = sparkSession.sql("SELECT * FROM FLIGHTS_DATA WHERE CANCELLED=0.0 LIMIT " + (delays.count() + cancelations.count()) )

      val stringIndexer = new StringIndexer()
        .setInputCol("CANCELLED")
        .setOutputCol("label")

      val vectorAssembler = new VectorAssembler()
        .setInputCols(TrainDataset.getClassificationInputCols())
        .setOutputCol("features")

      val Array(noDelaysTrainSet, noDelaysTestSet) =   noDelays.randomSplit(Array(0.55, 0.45), 11L)
      val Array(trainDelaysSet, testDelaysSet) =   delays.randomSplit(Array(0.65, 0.35), 11L)
      val Array(trainCancellationsSet, testCancellationsSet) =   cancelations.randomSplit(Array(0.8, 0.2), 11L)

      val pipelineTrainData = trainDelaysSet.union(trainCancellationsSet).union(noDelaysTrainSet)
      val pipelineTestData = testCancellationsSet.union(testDelaysSet).union(noDelaysTestSet)

      pipelineTrainData.cache()

      val randomForestClassifier = new RandomForestClassifier()
        .setNumTrees(10)
        .setLabelCol("CANCELLED")

      val stages = Array(vectorAssembler, randomForestClassifier)
      val pipeline = new Pipeline().setStages(stages)

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
          .save("/home/admin/randomForestModel")
      }

      //RDD[prediction, label]
      val rddEval: org.apache.spark.rdd.RDD[(Double, Double)] = predictions.select("CANCELLED", "prediction").rdd.map(row => ( row(1).toString().toDouble, row(0).toString().toDouble ))

      val metrics = new MulticlassMetrics(rddEval)
      val bw = new BufferedWriter(new FileWriter("/home/admin/results.txt"))

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

      val trainDataset = sparkSession.sql("SELECT * FROM FLIGHTS_DATA")

      val vectorAssembler = new VectorAssembler()
        .setInputCols(TrainDataset.getClassificationInputCols())
        .setOutputCol("features")

      val randomForestClassifier = new RandomForestClassifier()
        .setNumTrees(10)
        .setLabelCol("CANCELLED")

      val stages = Array(vectorAssembler, randomForestClassifier)
      val pipeline = new Pipeline().setStages(stages)

      this.model = pipeline.fit(trainDataset)

      if(saveModel)

        model
          .write
          .overwrite()
          .save("/home/admin/randomForestModel")

      println("***RANDOM FOREST MODEL TRAINED AND SAVED***")
    }

  }

  def predict(viewName: String): Unit = {

    //if(!Airports.isItLoaded())
      Airports.load()

   // if(!Airlines.isItLoaded())
      Airlines.load()

    this.loadExistingModel()

    println("***RANDOM FOREST PREDICTION***")

    //TestDataset.getDataFrame().show(200)

    val testData = sparkSession
      .sql("SELECT f.YEAR, f.MONTH, f.DAY_OF_MONTH, f.DAY_OF_WEEK, l.id AS OP_CARRIER_ID, a1.id AS ORIGIN, a2.id AS DESTINATION, f.CANCELLED, f.DISTANCE " +
        "FROM TEST_FLIGHTS_DATA AS f " +
        "INNER JOIN airlines AS l ON f.OP_CARRIER_ID=l.iata " +
        "INNER JOIN airports AS a1 ON f.ORIGIN=a1.iata " +
        "INNER JOIN airports AS a2 ON f.DESTINATION=a2.iata " +
        "WHERE f.DIVERTED!=1.0")

    //testData.show(200)

    val predictions = this.model.transform(testData)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("CANCELLED")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator
      .evaluate(predictions)

    //RDD[prediction, label]
    val rddEval: org.apache.spark.rdd.RDD[(Double, Double)] = predictions.select("CANCELLED", "prediction").rdd.map(row => ( row(1).toString().toDouble, row(0).toString().toDouble ))

    val metrics = new MulticlassMetrics(rddEval)
    val bw = new BufferedWriter(new FileWriter("/home/admin/results.txt"))

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
