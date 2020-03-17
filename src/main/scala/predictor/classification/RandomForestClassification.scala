package gr.upatras.ceid.ddcdm.predictor.classification

import gr.upatras.ceid.ddcdm.predictor.datasets.AirFlightsKaggleDataset
import gr.upatras.ceid.ddcdm.predictor.datasets.AirlinesDataset
import gr.upatras.ceid.ddcdm.predictor.datasets.AirportsKaggleDataset

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

object RandomForestClassification {

  def trainModel(): Unit = {
    AirportsKaggleDataset.load()
    AirlinesDataset.load()
    val airports = AirportsKaggleDataset.getAsDf()
    val airlines = AirlinesDataset.getAsDf()

    airports.show(20)
    //airlines.show(20)

    return
    val splitDf = AirFlightsKaggleDataset.getAsDf().randomSplit(Array(0.65, 0.35))

    val trainDf = splitDf(0)
    var testDf = splitDf(1)

    val labelIndexer = new StringIndexer()
     /* .setInputCol("MONTH")
      .setInputCol("DAY")
      .setInputCol("DAY_OF_WEEK")
      .setInputCol("DISTANCE")
      .setInputCol("ARRIVAL_DELAY")*/
      .setInputCol("CANCELLED")
      .setOutputCol("indexedLabel")
      .fit(trainDf)


    val featureIndexer = new VectorIndexer()
      .setInputCol("CANCELLED")
      .setOutputCol("indexedFeatures")
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

  }

}
