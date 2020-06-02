package gr.upatras.ceid.ddcdm.predictor.preprocess
import org.apache.spark.ml.feature.PCA
import gr.upatras.ceid.ddcdm.predictor.datasets.{ TrainDataset }
import gr.upatras.ceid.ddcdm.predictor.util.MLUtils
import org.apache.spark.sql.functions._
import org.apache.spark.ml._

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
      .setK(6)

    println("*** PCA REDUCTION TRANSFORM AND SAVE ***")
    val finaldf = pca
      .fit(tmp)
      .transform(tmp)

    // A UDF to convert VectorUDT to ArrayType
    val vecToArray = udf( (xs: linalg.Vector) => xs.toArray )

    // Add a ArrayType Column
    var dfArr = finaldf.withColumn("pca_features" , vecToArray(col("pca_features") ) )

    // Array of element names that need to be fetched
    // ArrayIndexOutOfBounds is not checked.
    // sizeof `elements` should be equal to the number of entries in column `features`
    val elements = Array("f1", "f2", "f3", "f4", "f5", "f6")

    // Create a SQL-like expression using the array
    val sqlExpr = elements.zipWithIndex.map{ case (alias, idx) => col("pca_features").getItem(idx).as(alias) }

    // Extract Elements from dfArr
    //dfArr.select(sqlExpr : _*)
    dfArr = dfArr.select( (col("*") +: sqlExpr) :_*)

    dfArr.select("CANCELLED", "ARR_DELAY", "f1", "f2", "f3", "f4", "f5", "f6")
      .repartition(1)
      .write
      .csv("/home/admin/pcaDataset")
  }
}
