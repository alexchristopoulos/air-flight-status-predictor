package gr.upatras.ceid.ddcdm.predictor.preprocess
import org.apache.spark.ml.feature.PCA
import gr.upatras.ceid.ddcdm.predictor.datasets.{ TrainDataset }
import gr.upatras.ceid.ddcdm.predictor.util.MLUtils

object PCAReduction {


  def reduceTrainDataset(): Unit = {

    println("*** PCA REDUCTION TRAIN DATASET ***")

    TrainDataset.load()

    println("*** PCA REDUCTION LOAD TRAIN DATASET ***")

    val tmp = MLUtils
      .getVectorAssember(
        TrainDataset.getClassificationInputCols(), "features"
      )
      .transform(TrainDataset.getDataFrame())

    println("*** PCA REDUCTION TRAIN DATASET GET FEATURE COL ***")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pca_features")
      .setK(5)

    println("*** PCA REDUCTION TRANSFORM AND SAVE ***")
    pca
      .fit(tmp)
      .transform(tmp)
      .select("CANCELLED", "ARR_DELAY", "pca_features")
      .repartition(1)
      .write
      .csv("/home/admin/pcaDataset")
  }
}
