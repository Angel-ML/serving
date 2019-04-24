package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object BucketedRandomProjectionLSHServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 1.0)),
      (1, Vectors.dense(1.0, -1.0)),
      (2, Vectors.dense(-1.0, -1.0)),
      (3, Vectors.dense(-1.0, 1.0))
    )).toDF("id", "features")

    val dfB = spark.createDataFrame(Seq(
      (4, Vectors.dense(1.0, 0.0)),
      (5, Vectors.dense(-1.0, 0.0)),
      (6, Vectors.dense(0.0, 1.0)),
      (7, Vectors.dense(0.0, -1.0))
    )).toDF("id", "features")

    val key = Vectors.dense(1.0, 0.0)

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = brp.fit(dfA)

    // Feature Transformation
    println("The hashed dataset where hashed values are stored in the column 'hashes':")
    model.transform(dfA).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate
    // similarity join.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxSimilarityJoin(transformedA, transformedB, 1.5)`
    println("Approximately joining dfA and dfB on Euclidean distance smaller than 1.5:")
//    model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
//      .select(col("datasetA.id").alias("idA"),
//        col("datasetB.id").alias("idB"),
//        col("EuclideanDistance")).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate nearest
    // neighbor search.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxNearestNeighbors(transformedA, key, 2)`
    println("Approximately searching dfA for 2 nearest neighbors of the key:")
    model.approxNearestNeighbors(dfA, key, 2).show()

    val res = trans(model)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: BucketedRandomProjectionLSHModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[BucketedRandomProjectionLSHServingModel]
    val rowsFeatures = new Array[SRow](4)
    val training = Seq(
      (4, Vectors.dense(1.0, 0.0)),
      (5, Vectors.dense(-1.0, 0.0)),
      (6, Vectors.dense(0.0, 1.0)),
      (7, Vectors.dense(0.0, -1.0))
    )
    for (i <- 0 until rowsFeatures.length) {
      rowsFeatures(i) = new SRow(Array(training(i)._2))
    }

//    val schema = new StructType().add(new StructField(model.getInputCol, new VectorUDT, true))
    val dataset = transModel.prepareData(rowsFeatures)
    transModel.transform(dataset)
  }
}
