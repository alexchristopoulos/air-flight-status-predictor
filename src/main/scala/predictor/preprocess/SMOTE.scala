package gr.upatras.ceid.ddcdm.predictor.preprocess

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ Param, ParamMap }
import org.apache.spark.ml.util.{ DefaultParamsReadable, DefaultParamsWritable, Identifiable }
import org.apache.spark.sql.{ DataFrame, Dataset }
import org.apache.spark.sql.types.StructType

class SMOTE extends Transformer with DefaultParamsWritable{

  //def this() = this(Identifiable.randomUID("SMOTE"))
  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def setfeatureParam(value: String): this.type = set(outputCol, value)
  def setlabelParam(value: String): this.type = set(outputCol, value)
  def setpercentOverParam(value: String): this.type = set(outputCol, value)
  def setBucketLengthParam(value: String): this.type = set(outputCol, value)
  def setNumHashTablesParam(value: String): this.type = set(outputCol, value)
  def getOutputCol: String = getOrDefault(outputCol)

  // val uid = Identifiable.randomUID("SMOTE-UID")
  override val uid: String = Identifiable.randomUID("SMOTE")

  val inputCol = new Param[String](this, "inputCol", "input column")
  val outputCol = new Param[String](this, "outputCol", "output column")

  val featureParam = new Param[String](this, "feature", "feature column")
  val labelParam = new Param[String](this, "label", "label column")
  val percentOverParam = new Param[Int](this, "percentOver", "percentOver")
  val BucketLengthParam = new Param[Int](this, "BucketLength", "BucketLength")
  val NumHashTablesParam = new Param[Int](this, "NumHashTables", "NumHashTables")

  override def transform(inputFrame: Dataset[_]): DataFrame = {

    val outCol = extractParamMap.getOrElse(outputCol, "output")
    val inCol = extractParamMap.getOrElse(inputCol, "input")
    val feature = extractParamMap.getOrElse(featureParam, "feature")
    val percentOver = extractParamMap.getOrElse(percentOverParam, 200)
    val label = extractParamMap.getOrElse(labelParam, "label")
    val BucketLength = extractParamMap.getOrElse(BucketLengthParam, 3)
    val NumHashTables  = extractParamMap.getOrElse(NumHashTablesParam, 4)

    SmoteImpl.Smote(inputFrame.toDF(), feature, label, percentOver, BucketLength, NumHashTables)
  }

  override def copy(extra: ParamMap): SMOTE = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType = schema
}

object SMOTE extends DefaultParamsReadable[SMOTE] {
  override def load(path: String): SMOTE = super.load(path)

}

