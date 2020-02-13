package predictor.spark

import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD;

class SparkClustering(sparkConf: SparkConf, sparkContext: SparkContext) {

  def kMeansClustering(): KMeansModel = {

    var kMeansModel: KMeansModel = null

    try {
      val textFile = this.sparkContext
        .textFile(config.sparkDatasetDir + "/iris.data");

      val parsedData = textFile.map(x =>
        Vectors.dense(Array(
          x.toString().split(",")(0).toDouble,
          x.toString().split(",")(1).toDouble,
          x.toString().split(",")(2).toDouble,
          x.toString().split(",")(3).toDouble
        ))
      ).cache()

      kMeansModel = KMeans.train(parsedData, 3, 5)
      //kMeansModel.save(sparkContext, "/home/admin/model.txt")

      var rdd: RDD[Array[String]] = textFile.map(x => x.split(","))

      var i: Int = 0;
      val predictions = rdd.map(row => {
        i += 1
        (i.toString(),
          row(4),
          kMeansModel.predict(Vectors.dense(row(0).toDouble, row(1).toDouble, row(2).toDouble, row(3).toDouble))
        )
      })

      println("***** K MEANS CLUSTERING RESULTS ON IRIS DATASET *****")
      println("Id,Label,Cluster")
      predictions.foreach(println)

      println("_______________________________________________________")
      println(kMeansModel.clusterCenters.foreach(println))
      println(kMeansModel.distanceMeasure)
      println(kMeansModel.trainingCost.toString())

    } catch {
      case exc: Exception => println(exc.toString())
    }


    return kMeansModel
  }

}
