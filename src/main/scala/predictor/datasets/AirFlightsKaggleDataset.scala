package predictor.datasets

import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import gr.upatras.ceid.ddcdm.predictor.util.FuncOperators

//air flights 2015 dataset
object AirFlightsKaggleDataset {

  private var datasetDf: DataFrame = _

  private val struct = StructType(
    StructField("iata", StringType, false) ::
      StructField("name", StringType, false) :: Nil)

  def load(sparkContext: SparkContext, sparkSession: SparkSession): Unit = {

    this.datasetDf = sparkSession.createDataFrame(
      sparkContext
        .textFile(config.sparkDatasetDir + config.sparkDatasetPredictionAirlines)
        .mapPartitionsWithIndex(FuncOperators.removeFirstLine)
        .map(FuncOperators.csvStringRowToRow),
      this.struct)

    this.datasetDf.as("airlines")
    this.datasetDf.createOrReplaceTempView("airlines")
  }

  def getAsDf(): DataFrame = {

    return this.datasetDf
  }
}
