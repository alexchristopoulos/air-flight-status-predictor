package gr.upatras.ceid.ddcdm.predictor.datasets

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType

import gr.upatras.ceid.ddcdm.predictor.spark.Spark

object TrainDataset {

  private var df: DataFrame = _
  private var struct: StructType = _

  private val selectedFeatures: Map[String, Tuple2[Int, String]] = Map(
    "YEAR" -> Tuple2(0, "Int"),
    "MONTH" -> Tuple2(1, "Int"),
    "DAY_OF_MONTH" -> Tuple2(2, "Int"),
    "DAY_OF_WEEK" -> Tuple2(3, "Int"),
    "OP_CARRIER" -> Tuple2(4, "Int"),
    "TAIL_NUM" -> Tuple2(5, "Int"),
    "OP_CARRIER_FL_NUM" -> Tuple2(6, "Int"),
    "ORIGIN" -> Tuple2(7, "Int"),
    "DEST" -> Tuple2(8, "Int"),
    "DEP_DELAY_NEW" -> Tuple2(9, "Int"),
    "TAXI_OUT" -> Tuple2(10, "Int"),
    "WHEELS_OFF" -> Tuple2(11, "Int"),
    "WHEELS_ON" -> Tuple2(12, "Int"),
    "TAXI_IN" -> Tuple2(13, "Int"),
    "ARR_DELAY_NEW" -> Tuple2(14, "Int"),
    "CANCELLED" -> Tuple2(15, "Int"),
    "CANCELLATION_CODE" -> Tuple2(16, "Int"),
    "DIVERTED" -> Tuple2(17, "Int"),
    "AIR_TIME" -> Tuple2(18, "Int"),
    "FLIGHTS" -> Tuple2(19, "Int"),
    "DISTANCE" -> Tuple2(20, "Int")
  )

  def load(): Unit = {

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

}
