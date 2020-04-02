package gr.upatras.ceid.ddcdm.predictor.util

import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
object FuncOperators {

  //used after sc.textFile for Csv formatted files to remove the first line that is the header
  def removeFirstLine = (idx: Int, iter: Iterator[String]) => if (idx == 0) iter.drop(1) else iter

  //Used to convert a Array[String] with comma delimited columns to Seq[Row] object
  def csvStringRowToRow: String => Row = (line: String) => Row.fromSeq(line.split(",").toSeq)

  //Used to convert a Array[String] with comma delimited columns to Row object given Types of variables for a given row
  def csvStringRowToRowType: (String, Map[Int, String]) => Row = (line:String, paramDef: Map[Int, String]) => {

    var tmp =  Map[Int, Any]()
    val tokens = line.split(",")
    //val map: Map[Int, Int] = Map(1 -> 2, 2 -> 3)

    paramDef.foreach((mapEntry) => {
      mapEntry._2 match {
        case "String" => {
          tmp += mapEntry._1 -> tokens(mapEntry._1).trim()
        }
        case "Int" => {
          tmp += mapEntry._1 -> tokens(mapEntry._1).trim().toInt
        }
        case "Double" => {
          tmp += mapEntry._1 -> tokens(mapEntry._1).trim().toDouble
        }
      }
    })

    tmp = scala
    .collection
    .immutable
    .ListMap(tmp.toSeq.sortBy(_._1):_*).toMap//asc sort by key

    //tmp.foreach(x => println(x.toString()))
    //println(line)
    Row.fromSeq(tmp.valuesIterator.toList.toSeq)
  }

  def specialOne: String => Row = (line: String) => {

    var tmp =  new ListBuffer[Any]()
    val tokens = line.split(",")

    var YEAR = tokens(0).toInt
    var MONTH = tokens(1).toInt
    var DAY = tokens(2).toInt
    var DAY_OF_WEEK = tokens(3).toInt
    var AIRLINE = tokens(4)
    var FLIGHT_NUMBER = tokens(5)
    var TAIL_NUMBER = tokens(6)
    var ORIGIN_AIRPORT = tokens(7)
    var DESTINATION_AIRPORT = tokens(8)
    var SCHEDULED_DEPARTURE = tokens(9)
    var DEPARTURE_TIME = null
    var DEPARTURE_DELAY = null
    var TAXI_OUT = null
    var WHEELS_OFF = null
    var SCHEDULED_TIME = null
    var ELAPSED_TIME = null
    var AIR_TIME = null
    var DISTANCE = null
    var WHEELS_ON = null
    var TAXI_IN = null
    var SCHEDULED_ARRIVAL = null
    var ARRIVAL_TIME = null
    var ARRIVAL_DELAY = null
    var DIVERTED = null
    var CANCELLED = null
    var CANCELLATION_REASON = null
    var AIR_SYSTEM_DELAY = null
    var SECURITY_DELAY = null
    var AIRLINE_DELAY = null
    var LATE_AIRCRAFT_DELAY = null
    var WEATHER_DELAY = null

    //tmp += YEAR
    tmp += MONTH
    tmp += DAY
    tmp += DAY_OF_WEEK
    tmp += AIRLINE
    tmp += ORIGIN_AIRPORT
    tmp += DESTINATION_AIRPORT

    tokens.length match {
      case 25 => {

        tmp += tokens(17).toInt
        tmp += 0
        tmp += 0

        //println(" NOT A DELAYED CANCELLED FLIGHT ")

        Row.fromSeq(tmp.toList.toSeq)
      }
      case 26 => {
       // println("A CANCELLED FLIGHT ")

        tmp += tokens(17).toInt
        tmp += 9999999
        tmp += 2

        Row.fromSeq(tmp.toList.toSeq)
      }
      case 31 => {
        //println(" A DELAYED FLIGHT ")

        tmp += tokens(17).toInt
        tmp += tokens(22).toInt

        tmp += 1

        Row.fromSeq(tmp.toList.toSeq)
      }
      case _ => {

        Row.empty
      }
    }
  }
  //Used to convert a Array[String] with comma delimited columns to Seq[Row] object for tripadvisor reviews dataset
  def csvStringRowToRowTripAdvReviews: String => Row = (line: String) => {

    try {

      val tokens = line.split(",")
      var id: Int = -1
      var iata: String = ""
      var name: String = ""
      var rating: Double = 0.0
      var numOfReviews: Int = 0

      println(line)
      if (tokens.size == 5) {

        id = tokens(0).replace("\"", "").toInt
        iata = tokens(1)
        name = tokens(2)
        rating = tokens(3).trim().split(" ")(0).toDouble / 5.0
        numOfReviews = tokens(4).replace(".", "").replace("\"","").replace(",", "").toInt

        Row.fromSeq(Seq(id, iata, name, rating, numOfReviews))
      } else {

        id = tokens(0).replace("\"", "").toInt
        iata = tokens(1)
        name = tokens(2)
        rating = tokens(4).trim().split(" ")(0).toDouble / 5.0
        numOfReviews = tokens(5).replace(".", "").replace("\"","").replace(",", "").toInt

        Row.fromSeq(Seq(id, iata, name, rating, numOfReviews))
      }

    } catch {
      case x: Exception => {

        x.printStackTrace()
        println("*@# Reading Exception >> " + x.toString())
        Row.fromSeq(Seq())
      }
    }
  }
}
