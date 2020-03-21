package gr.upatras.ceid.ddcdm.predictor.classification

import gr.upatras.ceid.ddcdm.predictor.datasets.AirFlightsKaggleDataset
import gr.upatras.ceid.ddcdm.predictor.datasets.AirlinesDataset
import gr.upatras.ceid.ddcdm.predictor.datasets.AirportsKaggleDataset
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, RFormula, StringIndexer, VectorIndexer}
import gr.upatras.ceid.ddcdm.predictor.spark.Spark

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
      .randomSplit(Array(0.55, 0.45))


    val trainDf = splitDataset(0)
    val testDf = splitDataset(1)

    println("Split dataset!")

    /*val formula = new RFormula()
      .setFormula("CANCELLED ~ DAY + DAY_OF_WEEK + AIRLINE_ID + ORIGIN + DESTINATION + ARRIVAL_DELAY + DISTANCE")
      .setFeaturesCol("features")
      .setLabelCol("label")*/

    /**
      * 362035 in class 0 >> no delay
      * 95951 in class 1 >> delay
      * 11982 in class 2 >> cancelled
      */


    val delays = sparkSession.sql("SELECT * FROM FLIGHTS WHERE CANCELLED=1")
    val cancelations = sparkSession.sql("SELECT * FROM FLIGHTS WHERE CANCELLED=2")
    val noDelays = sparkSession.sql("SELECT * FROM FLIGHTS WHERE CANCELLED=0 LIMIT " + (delays.count() + cancelations.count()).toString())


    //val trainrf = formula.fit(trainDf).transform(trainDf)
    //val testrf = formula.fit(testDf).transform(testDf)

    /*import org.apache.spark.ml.classification.NaiveBayes
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

    val model = new NaiveBayes()
      .fit(trainrf)

    val predictions = model
      .transform(testrf)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator
      .evaluate(predictions)

    println(s"Test set accuracy = $accuracy")*/

    //val rd = new RandomForestClassifier()
     // .setNumTrees(10)
      //.setFeaturesCol(Param[])
      //.setPredictionCol("CANCELLED")
/*
    val labelIndexer = new StringIndexer()
      .setInputCol("MONTH")
      .setInputCol("DAY")
      .setInputCol("DAY_OF_WEEK")
      .setInputCol("ORIGIN")
      .setInputCol("DESTINATION")
      .setInputCol("AIRLINE_ID")
      .setInputCol("DISTANCE")
      .setInputCol("ARRIVAL_DELAY")
      //.setInputCol("CANCELLED")
      .setOutputCol("CANCELLED")
      .fit(trainDf)


    val featureIndexer = new VectorIndexer()
      .setInputCol("MONTH")
      .setInputCol("DAY")
      .setInputCol("DAY_OF_WEEK")
      .setInputCol("ORIGIN")
      .setInputCol("DESTINATION")
      .setInputCol("AIRLINE_ID")
      .setInputCol("DISTANCE")
      .setInputCol("ARRIVAL_DELAY")
      //.setInputCol("CANCELLED")
      .setOutputCol("CANCELLED")
      .setMaxCategories(3)
      .fit(trainDf)

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainDf)

    // Make predictions.
    val predictions = model.transform(testDf)


    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${(1.0 - accuracy)}")

    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println(s"Learned classification forest model:\n ${rfModel.toDebugString}")
*/
  }

}
