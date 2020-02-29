package predictor.datasets

import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.sql.types._
import gr.upatras.ceid.ddcdm.predictor.util.FuncOperators

//air flights 2015 dataset
object AirFlightsKaggleDataset {

  private var datasetDf: DataFrame = _

  private val struct = StructType(
    StructField("YEAR", IntegerType, false) ::
      StructField("MONTH", IntegerType, false) ::
      StructField("DAY", IntegerType, false)::
      StructField("DAY_OF_WEEK", IntegerType, false) ::
      StructField("AIRLINE", StringType, false)::
      StructField("FLIGHT_NUMBER", IntegerType, false) ::
      StructField("TAIL_NUMBER", IntegerType, false)::
      StructField("ORIGIN_AIRPORT", StringType, false) ::
      StructField("DESTINATION_AIRPORT", StringType, false)::
      StructField("SCHEDULED_DEPARTURE", StringType, false) ::
      StructField("DEPARTURE_TIME", StringType, false)::
      StructField("DEPARTURE_DELAY", StringType, false)::
      StructField("TAXI_OUT", StringType, false) ::
      StructField("WHEELS_OFF", StringType, false)::
      StructField("SCHEDULED_TIME", StringType, false)::
      StructField("ELAPSED_TIME", StringType, false) ::
      StructField("AIR_TIME", StringType, false)::
      StructField("DISTANCE", StringType, false)::
      StructField("WHEELS_ON", StringType, false) ::
      StructField("TAXI_IN", StringType, false)::
      StructField("SCHEDULED_ARRIVAL", StringType, false) ::
      StructField("ARRIVAL_TIME", StringType, false)::
      StructField("ARRIVAL_DELAY", StringType, false)::
      StructField("DIVERTED", StringType, false) ::
      StructField("CANCELLED", StringType, false)::
      StructField("CANCELLATION_REASON", StringType, false)::
      StructField("AIR_SYSTEM_DELAY", StringType, false) ::
      StructField("SECURITY_DELAY", StringType, false)::
      StructField("AIRLINE_DELAY", StringType, false)::
      StructField("LATE_AIRCRAFT_DELAY", StringType, false) ::
      StructField("WEATHER_DELAY", StringType, false)::
      Nil)

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
