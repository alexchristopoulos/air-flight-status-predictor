package gr.upatras.ceid.ddcdm.predictor.datasets

import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.sql.types._
import gr.upatras.ceid.ddcdm.predictor.util.FuncOperators
import gr.upatras.ceid.ddcdm.predictor.spark.Spark

//air flights 2015 dataset
object AirFlightsKaggleDataset {

  private var datasetDf: DataFrame = _

  private val struct = StructType(
   // StructField("YEAR", StringType, true) ::
      StructField("MONTH", IntegerType, true) ::
      StructField("DAY", IntegerType, true)::
      StructField("DAY_OF_WEEK", IntegerType, true) ::
      StructField("AIRLINE", StringType, true)::
  //    StructField("FLIGHT_NUMBER", StringType, true) ::
  //    StructField("TAIL_NUMBER", StringType, true)::
      StructField("ORIGIN_AIRPORT", StringType, true) ::
      StructField("DESTINATION_AIRPORT", StringType, true)::
   //   StructField("SCHEDULED_DEPARTURE", StringType, true) ::
    //  StructField("DEPARTURE_TIME", StringType, true)::
    //  StructField("DEPARTURE_DELAY", StringType, true)::
    //  StructField("TAXI_OUT", StringType, true) ::
    //  StructField("WHEELS_OFF", StringType, true)::
    //  StructField("SCHEDULED_TIME", StringType, true)::
    //  StructField("ELAPSED_TIME", StringType, true) ::
    //  StructField("AIR_TIME", StringType, true)::
      StructField("DISTANCE", IntegerType, true)::
    //  StructField("WHEELS_ON", StringType, true) ::
    //  StructField("TAXI_IN", StringType, true)::
     // StructField("SCHEDULED_ARRIVAL", StringType, true) ::
     // StructField("ARRIVAL_TIME", StringType, true)::
      StructField("ARRIVAL_DELAY", IntegerType, true)::
     // StructField("DIVERTED", StringType, true) ::
      StructField("CANCELLED", IntegerType, true)::
     /* StructField("CANCELLATION_REASON", StringType, true)::
      StructField("AIR_SYSTEM_DELAY", StringType, true) ::
      StructField("SECURITY_DELAY", StringType, true)::
      StructField("AIRLINE_DELAY", StringType, true)::
      StructField("LATE_AIRCRAFT_DELAY", StringType, true) ::
      StructField("WEATHER_DELAY", StringType, true)::*/
      Nil)

  def load(): Unit = {

    println("***** LOADING DATASET " + config.sparkDatasetDir + config.sparkDatasetPredictionFlights + " *****")

    val t = Spark
      .getSparkContext()
      .textFile(config.sparkDatasetDir + config.sparkDatasetPredictionFlights)
      .mapPartitionsWithIndex(FuncOperators.removeFirstLine)
      .map(FuncOperators.specialOne)

    this.datasetDf = Spark
      .getSparkSession()
      .createDataFrame(t, this.struct)

    /*this.datasetDf = sparkSession.createDataFrame(
      sparkContext
        .textFile(config.sparkDatasetDir + config.sparkDatasetPredictionFlights)
        .mapPartitionsWithIndex(FuncOperators.removeFirstLine)
        .map(FuncOperators.specialOne),
      this.struct)*/

    this.datasetDf.as("flights")
    this.datasetDf.createOrReplaceTempView("flights")
  }

  def getAsDf(): DataFrame = {

    return this.datasetDf
  }
}
