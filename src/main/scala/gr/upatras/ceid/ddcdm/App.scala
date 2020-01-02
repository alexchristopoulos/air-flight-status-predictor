package gr.upatras.ceid.ddcdm;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;


object app {

    def main(args: Array[String]): Unit = {
        
        var sparkConf = new SparkConf()
            .setMaster("[*]")
            .setAppName("AIR FLIGHT STATUS PREDICTOR");

        var sparkContext = new SparkContext(sparkConf);

        println("hello world");
    }
}