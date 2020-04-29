package gr.upatras.ceid.ddcdm.predictor.classification

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.{ RandomForestClassifier, GBTClassifier, MultilayerPerceptronClassifier, NaiveBayes }
import org.apache.spark.ml.Pipeline

object Classification {

  val trainAndOrTest = (trainAndTest: Boolean, saveModel: Boolean, classifier: PipelineStage) => {

    var classifierCast = _

    if(classifier.isInstanceOf[RandomForestClassifier]){
      classifierCast = classifier.asInstanceOf[RandomForestClassifier]
    } else if(classifier.isInstanceOf[NaiveBayes]) {
      classifierCast = classifier.asInstanceOf[NaiveBayes]
    } else if(classifier.isInstanceOf[GBTClassifier]) {
      classifierCast = classifier.asInstanceOf[GBTClassifier]
    } else if(classifier.isInstanceOf[MultilayerPerceptronClassifier]) {
      classifierCast = classifier.asInstanceOf[MultilayerPerceptronClassifier]
    } else {
      throw new Exception("Invalid classification model!")
    }

    val pipeline = new Pipeline()

    if(trainAndTest) {

    } else {

    }

  }

}
