package gr.upatras.ceid.ddcdm.predictor.spark

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import gr.upatras.ceid.ddcdm.predictor.config.config;

class Spark {

  var sparkConf:SparkConf = _;
  var sparkContext:SparkContext = _;

  locally{
    this.sparkConf = new SparkConf()
      .setAppName(config.sparkConfSetAppName)
      .setMaster(config.sparkConfSetMaster);

    this.sparkContext = new SparkContext(this.sparkConf);
  }

}
