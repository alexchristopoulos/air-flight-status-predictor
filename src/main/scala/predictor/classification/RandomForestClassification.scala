package gr.upatras.ceid.ddcdm.predictor.classification

import gr.upatras.ceid.ddcdm.predictor.datasets.AirFlightsKaggleDataset
import gr.upatras.ceid.ddcdm.predictor.datasets.AirlinesDataset
import gr.upatras.ceid.ddcdm.predictor.datasets.AirportsKaggleDataset
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{ RandomForestClassifier }
import org.apache.spark.ml.feature.{ RFormula, StringIndexer, VectorAssembler }
import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object RandomForestClassification {

  private val sparkSession = Spark.getSparkSession()

  def trainModel(): Unit = {

    AirportsKaggleDataset.load()
    AirlinesDataset.load()
    AirFlightsKaggleDataset.load()

    val splitDataset = sparkSession
      .sql("SELECT f.DAY, f.DAY_OF_WEEK, l.id AS AIRLINE_ID, a1.id AS ORIGIN, a2.id AS DESTINATION, f.ARRIVAL_DELAY, f.CANCELLED, f.DISTANCE " +
        "FROM flights AS f " +
        "INNER JOIN airlines AS l ON l.iata=f.AIRLINE " +
        "INNER JOIN airports AS a1 ON f.ORIGIN_AIRPORT=a1.iata " +
        "INNER JOIN airports AS a2 ON f.DESTINATION_AIRPORT=a2.iata")
        .createOrReplaceTempView("FLIGHTS_DATA")

    val delays = sparkSession.sql("SELECT * FROM FLIGHTS_DATA WHERE CANCELLED=1")
    val cancelations = sparkSession.sql("SELECT * FROM FLIGHTS_DATA WHERE CANCELLED=2")
    val noDelays = sparkSession.sql("SELECT * FROM FLIGHTS_DATA WHERE CANCELLED=0 ")

    val stringIndexer = new StringIndexer()
      .setInputCol("CANCELLED")
      .setOutputCol("label")

    val featuresNames = Array("DAY", "DAY_OF_WEEK", "AIRLINE_ID", "ORIGIN", "DESTINATION", "ARRIVAL_DELAY", "DISTANCE")

    val vectorAssembler = new VectorAssembler()
      .setInputCols(featuresNames)
      .setOutputCol("features")

    var Array(pipelineTrainData, pipelineTestData) = delays
      .union(cancelations)
      .union(noDelays)
      .randomSplit(Array(0.65, 0.35), 11L)

    pipelineTrainData.cache()

    val randomForestClassifier = new RandomForestClassifier()
        .setNumTrees(10)

    val stages = Array(vectorAssembler, stringIndexer, randomForestClassifier)
    val pipeline = new Pipeline().setStages(stages)

    val model = pipeline.fit(pipelineTrainData)

    val predictions = model.transform(pipelineTestData)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator
      .evaluate(predictions)

    model.write.overwrite().save("/home/admin/randomForestModel")

    val rddEval: org.apache.spark.rdd.RDD[(Double, Double)] = predictions.rdd.map(row => ( row(12).toString().toDouble, row(9).toString().toDouble ))

    val metrics = new MulticlassMetrics(rddEval)

    println("LABEL  PRECISION   RECALL  F-MEASURE")
    metrics.labels.foreach(label => {
      println(label.toString() + "  " + metrics.precision(label).toString() + "  " + metrics.recall(label).toString() + "   " + metrics.fMeasure(label).toString())
    })

    println("\n\nWEIGHTED_PRECISION   WEIGHTED_RECALL   WEIGHTED_F1")
    println(metrics.weightedPrecision + "   " + metrics.weightedRecall + "   " + metrics.weightedFMeasure + "\n")

    println(s"Test set accuracy = $accuracy")



  }

}
