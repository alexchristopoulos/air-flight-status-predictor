package gr.upatras.ceid.ddcdm.predictor.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql._

object Spark {

  private val sparkConf: SparkConf = new SparkConf()
    .setAppName(config.sparkConfSetAppName)
    .setMaster(config.sparkConfSetMaster)

  private val sparkContext: SparkContext = new SparkContext(this.sparkConf)
  private val sparkSession = SparkSession
    .builder()
    .appName(config.sparkConfSetAppName + " ***SQL***")
    .getOrCreate()

  def exit(): Unit = {
    this.sparkSession.stop()
    this.sparkContext.stop()
  }

  def getSparkContext(): SparkContext = {
    return this.sparkContext
  }

  def getSparkSession(): SparkSession = {

    return this.sparkSession
  }




}
