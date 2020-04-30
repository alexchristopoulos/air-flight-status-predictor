package gr.upatras.ceid.ddcdm.predictor.classification

import gr.upatras.ceid.ddcdm.predictor.datasets.{Airlines, Airports, TestDataset, TrainDataset, TripadvisorAirlinesReviewsDataset}

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{RFormula, StringIndexer, VectorAssembler}
import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import java.io._

object RandomForestClassification {

  val RFClassifier = new RandomForestClassifier()
    .setNumTrees(10)
    .setLabelCol("CANCELLED")

}
