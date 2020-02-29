package gr.upatras.ceid.ddcdm.predictor.util

import org.apache.spark.sql.Row

object FuncOperators {

  //used after sc.textFile for Csv formatted files to remove the first line that is the header
  def removeFirstLine = (idx: Int, iter: Iterator[String]) => if (idx == 0) iter.drop(1) else iter

  //Used to convert a Array[String] with comma delimited columns to Seq[Row] object
  def csvStringRowToRow: String => Row = (line: String) => Row.fromSeq(line.split(",").toSeq)

  //Used to convert a Array[String] with comma delimited columns to Seq[Row] object for tripadvisor reviews dataset
  def csvStringRowToRowTripAdvReviews: String => Row = (line: String) => {

    try {
      val tokens = line.split(",")
      var iata: String = ""
      var name: String = ""
      var rating: Double = 0.0
      var numOfReviews: Int = 0

      if (tokens.size == 4) {

        iata = tokens(0)
        name = tokens(1)
        rating = tokens(2).trim().split(" ")(0).toDouble / 5.0
        numOfReviews = tokens(3).replace(".", "").replace(",", "").toInt

        Row.fromSeq(Seq(iata, name, rating, numOfReviews))
      } else {

        iata = tokens(0)
        name = tokens(1)
        rating = tokens(3).trim().split(" ")(0).toDouble / 5.0
        numOfReviews = tokens(4).replace(".", "").replace(",", "").toInt

        Row.fromSeq(Seq(iata, name, rating, numOfReviews))
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
