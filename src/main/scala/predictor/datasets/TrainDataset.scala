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
    "OP_CARRIER" -> Tuple2(4, "String"), //airline iata code
    "TAIL_NUM" -> Tuple2(5, "String"),
    "OP_CARRIER_FL_NUM" -> Tuple2(6, "Int"), //flight number for op carrier
    "ORIGIN" -> Tuple2(7, "String"), //origin airpotr iata code
    "DEST" -> Tuple2(8, "String"), //destination airpotr iata code
    "DEP_DELAY_NEW" -> Tuple2(9, "Double"), //departure delay in minutes
    "TAXI_OUT" -> Tuple2(10, "Double"),
    "WHEELS_OFF" -> Tuple2(11, "Int"),
    "WHEELS_ON" -> Tuple2(12, "Int"),
    "TAXI_IN" -> Tuple2(13, "Double"),
    "ARR_DELAY_NEW" -> Tuple2(14, "Double"),
    "CANCELLED" -> Tuple2(15, "Double"),
    "CANCELLATION_CODE" -> Tuple2(16, "String"),
    "DIVERTED" -> Tuple2(17, "Double"),
    "AIR_TIME" -> Tuple2(18, "Double"),
    "FLIGHTS" -> Tuple2(19, "Double"),
    "DISTANCE" -> Tuple2(20, "Double")
  )

  def load(): Unit = {

    this.initStruct()

    this.df = Spark
      .getSparkSession()
      .createDataFrame(
        Spark
          .getSparkContext()
          .textFile(config.sparkDatasetDir + config.sparkTrainDataset)
          .mapPartitionsWithIndex(FuncOperators.removeFirstLine)
          .map(line => FuncOperators.csvStringRowToRowType(line, this.getTypeMapping())),
        this.struct
      )

    this.df.as("trainData")
    this.df.createOrReplaceGlobalTempView("trainData")
  }

  def getDataFrame(): DataFrame = {

    return this.df
  }

  private def initStruct(): Unit = {

    val tmp = new StructType()

    this.selectedFeatures.foreach((entry) => {
      entry._2._2 match {
        case "Int" => {
          tmp.add(StructField(entry._1, IntegerType, true))
        }
        case "String" => {
          tmp.add(StructField(entry._1, StringType, true))
        }
        case "Double" => {
          tmp.add(StructField(entry._1, DoubleType, true))
        }
      }
    })

    this.struct = tmp
  }

  private def getTypeMapping(): Map[Int, String] = Unit {

    var map: Map[Int, String] = Map()

    this.selectedFeatures.foreach((entry) => {

      map += (entry._2._1 -> entry._2._2)
    })

    return map
  }

}
