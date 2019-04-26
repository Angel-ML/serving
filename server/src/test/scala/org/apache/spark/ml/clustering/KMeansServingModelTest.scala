package org.apache.spark.ml.clustering

import java.util

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object KMeansServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    // Loads data.
    val dataset = spark.read.format("libsvm").load("data/sample_kmeans_data.txt")

    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    val res = trans(model)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: KMeansModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[KMeansServingModel]
    val rows = new Array[SRow](1)
    val size = 3
    val index = Array[Int](0, 1, 2)
    val value = Array[Double](0.1, 0.0, 0.0)
//    for (i <- 0 until rows.length) {
//      rows(i) = new SRow(Array(Vectors.sparse(size, index, value)))
//    }
    val data: util.Map[String, Vector] = new util.HashMap[String, Vector]
    data.put(model.getFeaturesCol, Vectors.sparse(size, index, value))

//    val schema = new StructType().add(new StructField(model.getFeaturesCol, new VectorUDT, true))
    val dataset = transModel.prepareData(data)
    transModel.transform(dataset)
  }
}
