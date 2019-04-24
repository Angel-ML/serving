package org.apache.spark.ml.clustering

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object GaussianMixtureServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    // Loads data
    val dataset = spark.read.format("libsvm").load("data/sample_kmeans_data.txt")

    // Trains Gaussian Mixture Model
    val gmm = new GaussianMixture()
      .setK(2)
    val model = gmm.fit(dataset)

    // output parameters of mixture model model
    for (i <- 0 until model.getK) {
      println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
        s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
    }

    val res = trans(model)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: GaussianMixtureModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[GaussianMixtureServingModel]
    val rows = new Array[SRow](1)
    val size = 3
    val index = Array[Int](0, 1, 2)
    val value = Array[Double](0.1, 0.0, 0.0)
    for (i <- 0 until rows.length) {
      rows(i) = new SRow(Array(Vectors.sparse(size, index, value)))
    }

//    val schema = new StructType().add(new StructField(model.getFeaturesCol, new VectorUDT, true))
    val dataset = transModel.prepareData(rows)
    transModel.transform(dataset)
  }
}
