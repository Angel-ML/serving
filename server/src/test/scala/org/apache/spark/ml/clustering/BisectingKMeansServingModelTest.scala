package org.apache.spark.ml.clustering

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object BisectingKMeansServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    // Loads data.
    val dataset = spark.read.format("libsvm").load("data/sample_kmeans_data.txt")

    // Trains a bisecting k-means model.
    val bkm = new BisectingKMeans().setK(2).setSeed(1)
    val model = bkm.fit(dataset)

    // Evaluate clustering.
    val cost = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $cost")

    // Shows the result.
    println("Cluster Centers: ")
    val centers = model.clusterCenters
    centers.foreach(println)

    val res = trans(model)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: BisectingKMeansModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[BisectingKMeansServingModel]
    val rows = new Array[SRow](1)
    val size = 3
    val index = Array[Int](0, 1, 2)
    val value = Array[Double](0.1, 0.0, 0.0)
    for (i <- 0 until rows.length) {
      rows(i) = new SRow(Array(Vectors.sparse(size, index, value)))
    }

    val schema = new StructType().add(new StructField(model.getFeaturesCol, new VectorUDT, true))
    val dataset = new SDFrame(rows)(schema)
    transModel.transform(dataset)
  }
}
