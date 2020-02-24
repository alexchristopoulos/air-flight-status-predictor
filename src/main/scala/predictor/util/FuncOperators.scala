package gr.upatras.ceid.ddcdm.predictor.util
import org.apache.spark.sql.Row

object FuncOperators {

  //used after sc.textFile for Csv formatted files to remove the first line that is the header
  def removeFirstLine = (idx: Int, iter: Iterator[String]) => if (idx == 0) iter.drop(1) else iter

  //Used to convert a Array[String] with comma delimited columns to Seq[Row] object
  def csvStringRowToRow = (line: String) => Row.fromSeq(line.split(",").toSeq)
}
