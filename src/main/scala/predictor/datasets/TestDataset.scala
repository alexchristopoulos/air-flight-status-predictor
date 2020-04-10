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

object TestDataset {

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
    //"ARR_DELAY_NEW" -> Tuple2(14, "Double"),
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

    this.df = Spark
      .getSparkSession()
      .createDataFrame(
        Spark
          .getSparkContext()
          .textFile(config.sparkDatasetDir + config.sparkTestDataset)
          .mapPartitionsWithIndex(FuncOperators.removeFirstLine)
          .map(line => FuncOperators.csvStringRowToRowType(line, typeMapping))
        , this.struct
      )

    this.df
      .as("TEST_FLIGHTS_DATA")
      .createOrReplaceTempView("TEST_FLIGHTS_DATA")
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
    return Array("YEAR", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK", "OP_CARRIER_ID", "ORIGIN", "DESTINATION", "DISTANCE")
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
              entry._2._1 -> StructField(entry._1, DoubleType, false)
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
