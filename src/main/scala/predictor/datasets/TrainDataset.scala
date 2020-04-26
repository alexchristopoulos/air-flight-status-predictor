package gr.upatras.ceid.ddcdm.predictor.datasets

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import gr.upatras.ceid.ddcdm.predictor.spark.Spark
import gr.upatras.ceid.ddcdm.predictor.config.config
import gr.upatras.ceid.ddcdm.predictor.util.FuncOperators

object TrainDataset {

  private var df: DataFrame = _
  private var struct: StructType = _

  private val selectedFeatures: Map[String, Tuple2[Int, String]] = Map(
    "YEAR" -> Tuple2(0, "Int"),
    "MONTH" -> Tuple2(1, "Int"),
    "DAY_OF_MONTH" -> Tuple2(2, "Int"),
    "DAY_OF_WEEK" -> Tuple2(3, "Int"),
    "OP_CARRIER_ID" -> Tuple2(4, "String"), //airline iata code
    //"TAIL_NUM" -> Tuple2(5, "String"),
    //"AIRLINE" -> Tuple2(6, "Int"), //flight number for op carrier
    "ORIGIN" -> Tuple2(7, "String"), //origin airpotr iata code
    "DESTINATION" -> Tuple2(8, "String"), //destination airpotr iata code
    //"DEP_DELAY_NEW" -> Tuple2(9, "Double"), //departure delay in minutes
    //"TAXI_OUT" -> Tuple2(10, "Double"),
    //"WHEELS_OFF" -> Tuple2(11, "Int"),
    //"WHEELS_ON" -> Tuple2(12, "Int"),
    //"TAXI_IN" -> Tuple2(13, "Double"),
    "ARR_DELAY_NEW" -> Tuple2(14, "Double"),
    "CANCELLED" -> Tuple2(15, "Cat;Can;Double"),
    //"CANCELLATION_CODE" -> Tuple2(16, "String"),
    "DIVERTED" -> Tuple2(17, "Double"),
    //"AIR_TIME" -> Tuple2(18, "Double"),
    //"FLIGHTS" -> Tuple2(19, "Double"),
    "DISTANCE" -> Tuple2(20, "Double")
  )

  def load(): Unit = {

    this.initStruct()

    val typeMapping = this.getTypeMapping()

    Spark
      .getSparkSession()
      .createDataFrame(
        Spark
          .getSparkContext()
          .textFile(config.sparkDatasetDir + config.sparkTrainDataset)
          .mapPartitionsWithIndex(FuncOperators.removeFirstLine)
          .map(line => FuncOperators.csvStringRowToRowType(line, typeMapping))
        , this.struct)
      .as("TRAIN_FLIGHTS_DATA")
      .createOrReplaceTempView("TRAIN_FLIGHTS_DATA")

    Airports.load()
    Airlines.load()
    TripadvisorAirlinesReviewsDataset.load()

    Spark
      .getSparkSession()
      .sql("SELECT CONCAT(f.DAY_OF_MONTH, '-', f.MONTH, '-', f.YEAR, '/' ,a1.id, '-', a1.iata) AS DATE_ORIGIN_ID, " +
        "f.MONTH, f.DAY_OF_MONTH, f.DAY_OF_WEEK, l.id AS OP_CARRIER_ID, a1.id AS ORIGIN, a2.id AS DESTINATION, f.CANCELLED, f.DISTANCE, f.ARR_DELAY_NEW AS ARR_DELAY, ar.rating as AIRLINE_RATING, ar.numOfReviews as NUM_OF_AIRLINE_REVIEWS " +
        "FROM TRAIN_FLIGHTS_DATA AS f " +
        "INNER JOIN airlines AS l ON f.OP_CARRIER_ID=l.iata " +
        "INNER JOIN airports AS a1 ON f.ORIGIN=a1.iata " +
        "INNER JOIN airports AS a2 ON f.DESTINATION=a2.iata " +
        "INNER JOIN airlineReviews AS ar ON f.OP_CARRIER_ID=ar.iata " +
        "WHERE f.DIVERTED!=1.0")
      .as("TRAIN_FLIGHTS_DATA")
      .createOrReplaceTempView("TRAIN_FLIGHTS_DATA")

    Spark
      .getSparkSession()
      .sql("SELECT DATE_ORIGIN_ID, COUNT(*) AS FLIGHTS_COUNT FROM TRAIN_FLIGHTS_DATA AS ff GROUP BY ff.DATE_ORIGIN_ID").createOrReplaceTempView("NUM_OF_FLIGHTS_PER_DATE_PER_ORIGIN")

    if(Airlines.avgDelayResourcesExist() == false){

      val avgDel = Spark
        .getSparkSession()
        .sql("SELECT OP_CARRIER_ID, AVG(ARR_DELAY) AS AIRLINE_MEAN_DELAY FROM TRAIN_FLIGHTS_DATA AS ff GROUP BY ff.OP_CARRIER_ID")

      avgDel.createOrReplaceTempView("MEAN_DELAY_AIRLINES")

      println("CREATED AND SAVING AVERAGE AIRLINE AVG DELAYS")
      Airlines.saveAvgDelays(avgDel)

    } else {

      Airlines.loadAvgDelays()
      println("LOADED AIRLINE AVG DELAYS")
    }

    this.df = Spark
      .getSparkSession()
      .sql("SELECT f.*, cf.FLIGHTS_COUNT AS FLIGHTS_COUNT, mf.AIRLINE_MEAN_DELAY FROM TRAIN_FLIGHTS_DATA AS f " +
      "INNER JOIN NUM_OF_FLIGHTS_PER_DATE_PER_ORIGIN AS cf ON f.DATE_ORIGIN_ID=cf.DATE_ORIGIN_ID " +
      "INNER JOIN MEAN_DELAY_AIRLINES AS mf ON f.OP_CARRIER_ID=mf.OP_CARRIER_ID")

    this.df
      .as("TRAIN_FLIGHTS_DATA")
      .createOrReplaceTempView("TRAIN_FLIGHTS_DATA")
  }

  def getDataFrame(): DataFrame = {

    return this.df
  }

  def getClassificationInputCols(): Array[String] = {

   /* var inputCols: String = ""

    this.selectedFeatures.foreach(entry => {

      if (
        entry._2._1 != 9 &&
          entry._2._1 != 10 &&
          entry._2._1 != 11 &&
          entry._2._1 != 12 &&
          entry._2._1 != 13 &&
          entry._2._1 != 14 &&
          entry._2._1 != 15 &&
          entry._2._1 != 16 &&
          entry._2._1 != 18 &&
          entry._2._1 != 19
      ) //This data is available only after a flight is completed

        inputCols = inputCols + entry._1 + " "

    });*/

    //return inputCols.trim();
    return Array("MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK", "OP_CARRIER_ID", "ORIGIN", "DESTINATION", "DISTANCE", "FLIGHTS_COUNT", "AIRLINE_MEAN_DELAY", "AIRLINE_RATING", "NUM_OF_AIRLINE_REVIEWS")
  }

  def getPredictionInputCols(): String = {

    var inputCols: String = ""

    this.selectedFeatures.foreach(entry => {

      if (
        entry._2._1 != 9 &&
          entry._2._1 != 10 &&
          entry._2._1 != 11 &&
          entry._2._1 != 12 &&
          entry._2._1 != 13 &&
          entry._2._1 != 14 &&
          entry._2._1 != 15 &&
          entry._2._1 != 16 &&
          entry._2._1 != 18 &&
          entry._2._1 != 19
      ) //This data is available only after a flight is completed
        inputCols = inputCols + entry._1 + " "
    });

    return inputCols.trim();
  }

  private def initStruct(): Unit = {

    this.struct = StructType(

      scala
        .collection
        .immutable
        .ListMap(this.selectedFeatures.map(entry => {
          entry._2._2 match {
            case "Int" => {
              entry._2._1 -> StructField(entry._1, IntegerType, false)
            }
            case "String" => {
              entry._2._1 -> StructField(entry._1, StringType, false)
            }
            case "Double" => {
              entry._2._1 -> StructField(entry._1, DoubleType, true)
            }
            case "Cat;Can;Double" => {
              entry._2._1 -> StructField(entry._1, DoubleType, false)
            }
          }
        })
          .toSeq.sortBy(_._1): _*)
        .valuesIterator
        .toArray
    )
  }

  private def getTypeMapping(): Map[Int, String] = {

    var map: Map[Int, String] = Map()

    this.selectedFeatures.foreach(entry => {

      map += (entry._2._1 -> entry._2._2)
    })

    return map
  }

}
