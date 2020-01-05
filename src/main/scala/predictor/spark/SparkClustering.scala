package predictor.spark

import gr.upatras.ceid.ddcdm.predictor.config.config
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

class SparkClustering(sparkConf: SparkConf, sparkContext: SparkContext) {

  def kMeansClustering(): KMeansModel = {

    println("COMING GEWEATTTTTTTTTTTTTTT");

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

      println("*****************************************************")
      textFile.map(x => x.split(",")(4)).foreach(println)



      println("******************************************************")

    } catch {
      case exc: Exception => println(exc.toString()+ " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    }


    return kMeansModel;
  }

}
