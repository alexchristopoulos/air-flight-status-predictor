package gr.upatras.ceid.ddcdm.predictor.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

object Spark {

  private var sparkConf: SparkConf = _
  private var sparkContext: SparkContext = _
  private var sparkSession: SparkSession = _

  locally {
    this.sparkConf = new SparkConf()
      .setAppName(config.sparkConfSetAppName)
      .setMaster(config.sparkConfSetMaster)

    this.sparkContext = new SparkContext(this.sparkConf)

    this.sparkSession = new SparkSession(this.sparkContext)
  }

  def exit(): Unit = {
    this.sparkContext.stop()
  }

  def getSparkContext(): SparkContext = {
    return this.sparkContext
  }

  def getSparkSession(): SparkSession = {


    return this.sparkSession
  }




}
