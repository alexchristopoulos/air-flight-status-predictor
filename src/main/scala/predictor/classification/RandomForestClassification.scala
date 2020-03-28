package gr.upatras.ceid.ddcdm.predictor.classification

import gr.upatras.ceid.ddcdm.predictor.datasets.AirFlightsKaggleDataset
import gr.upatras.ceid.ddcdm.predictor.datasets.AirlinesDataset
import gr.upatras.ceid.ddcdm.predictor.datasets.AirportsKaggleDataset
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, RFormula, StringIndexer, VectorIndexer}
import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

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

    /**
      * 362035 in class 0 >> no delay
      * 95951 in class 1 >> delay
      * 11982 in class 2 >> cancelled
      */

    val delays = sparkSession.sql("SELECT * FROM FLIGHTS_DATA WHERE CANCELLED=1")
    val cancelations = sparkSession.sql("SELECT * FROM FLIGHTS_DATA WHERE CANCELLED=2")
    val noDelays = sparkSession.sql("SELECT * FROM FLIGHTS_DATA WHERE CANCELLED=0 LIMIT " + ((delays.count() + cancelations.count())/3).toString())

    val splitCombined = delays
      .union(cancelations)
      .union(noDelays)
      .randomSplit(Array(0.7, 0.3))

    val formula = new RFormula()
      .setFormula("CANCELLED ~ DAY + DAY_OF_WEEK + AIRLINE_ID + ORIGIN + DESTINATION + ARRIVAL_DELAY + DISTANCE")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val trainrf = formula
      .fit(splitCombined(0))
      .transform(splitCombined(0))

    val testrf = formula
      .fit(splitCombined(1))
      .transform(splitCombined(1))

    val model = new RandomForestClassifier()
      .setNumTrees(10)
      .fit(trainrf)

    val predictions = model
      .transform(testrf)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator
      .evaluate(predictions)

    println(s"Test set accuracy = $accuracy")

  }

}
