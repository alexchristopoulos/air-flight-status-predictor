package gr.upatras.ceid.ddcdm

import gr.upatras.ceid.ddcdm.config
import scala.io.Source;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext._;
import org.apache.spark.SparkConf;

/**
  * @author ${Christopoulos Alexandros}
  *
  */
object App {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setMaster(config.default.sparkMasterConf)
      .setAppName(config.default.appName)

    val sparkContext = new SparkContext(sparkConf)
    parseExampleInput()
    parseExampleInput()
  }

  def parseExampleInput(): Unit = {
    println("Reading File....")
    for (line <- Source.fromFile(config.default.exampleInputFile).getLines) {
      for (col <- line.split(",")) {
        print(col, " || ")
      }
      println("\n")
    }
  }
}
