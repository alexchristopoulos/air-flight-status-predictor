package gr.upatras.ceid.ddcdm.predictor.preprocess

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SmoteImpl{

  def KNNCalculation(
                      dataFinal:org.apache.spark.sql.DataFrame,
                      feature:String,
                      reqrows:Int,
                      BucketLength:Int,
                      NumHashTables:Int,
                      labelCol: String):org.apache.spark.sql.DataFrame = {
    val b1 = dataFinal.withColumn("index", row_number().over(Window.partitionBy(labelCol).orderBy(labelCol)))
    val brp = new BucketedRandomProjectionLSH().setBucketLength(BucketLength).setNumHashTables(NumHashTables).setInputCol(feature).setOutputCol("values")
    val model = brp.fit(b1)
    val transformedA = model.transform(b1)
    val transformedB = model.transform(b1)
    val b2 = model.approxSimilarityJoin(transformedA, transformedB, 2000000000.0)
    require(b2.count > reqrows, println("Change bucket lenght or reduce the percentageOver"))
    val b3 = b2.selectExpr("datasetA.index as id1",
      "datasetA.feature as k1",
      "datasetB.index as id2",
      "datasetB.feature as k2",
      "distCol").filter("distCol>0.0").orderBy("id1", "distCol").dropDuplicates().limit(reqrows)

    return b3
  }

  def smoteCalc(key1: org.apache.spark.ml.linalg.Vector, key2: org.apache.spark.ml.linalg.Vector)={
    val resArray = Array(key1, key2)
    val res = key1.toArray.zip(key2.toArray.zip(key1.toArray).map(x => x._1 - x._2).map(_*0.2)).map(x => x._1 + x._2)
    resArray :+ org.apache.spark.ml.linalg.Vectors.dense(res)}

  def Smote(
             inputFrame:org.apache.spark.sql.DataFrame,
             feature:String,
             label:String,
             percentOver:Int,
             BucketLength:Int,
             NumHashTables:Int):org.apache.spark.sql.DataFrame = {
    val groupedData = inputFrame.groupBy(label).count
    require(groupedData.count == 2, println("Only 2 labels allowed"))
    val classAll = groupedData.collect()
    val minorityclass = if (classAll(0)(1).toString.toInt > classAll(1)(1).toString.toInt) classAll(1)(0).toString else classAll(0)(0).toString
    val frame = inputFrame.select(feature,label).where(label + " == " + minorityclass)
    val rowCount = frame.count
    val reqrows = (rowCount * (percentOver/100)).toInt
    val md = udf(smoteCalc _)
    val b1 = KNNCalculation(frame, feature, reqrows, BucketLength, NumHashTables, label)
    val b2 = b1.withColumn("ndtata", md(col("k1"), col("k2"))).select("ndtata")
    val b3 = b2.withColumn("AllFeatures", explode(col("ndtata"))).select("AllFeatures").dropDuplicates
    val b4 = b3.withColumn(label, lit(minorityclass).cast(frame.schema(1).dataType))
    return mergeMinorityDf(b4, inputFrame).dropDuplicates
  }

  private def mergeMinorityDf(minorDf: DataFrame, originalDf: DataFrame): DataFrame = {

    val cols1 = minorDf.columns.toSet
    val cols2 = originalDf.columns.toSet
    val total = cols1 ++ cols2 // union

    minorDf.select(expr(cols1, total):_*).union(originalDf.select(expr(cols2, total):_*))
  }

  private def expr(myCols: Set[String], allCols: Set[String]) = {
    allCols.toList.map(x => x match {
      case x if myCols.contains(x) => col(x)
      case _ => lit(null).as(x)
    })
  }
}