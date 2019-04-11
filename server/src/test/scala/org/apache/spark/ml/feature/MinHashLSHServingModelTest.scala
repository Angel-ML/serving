package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object MinHashLSHServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),
      (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))),
      (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "features")

    val dfB = spark.createDataFrame(Seq(
      (3, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (5, 1.0)))),
      (4, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (5, 1.0)))),
      (5, Vectors.sparse(6, Seq((1, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "features")

    val key = Vectors.sparse(6, Seq((1, 1.0), (3, 1.0)))

    val mh = new MinHashLSH()
      .setNumHashTables(5)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = mh.fit(dfA)

    // Feature Transformation
    println("The hashed dataset where hashed values are stored in the column 'hashes':")
    model.transform(dfA).show()


    println("Approximately joining dfA and dfB on Jaccard distance smaller than 0.6:")

    println("Approximately searching dfA for 2 nearest neighbors of the key:")
    model.approxNearestNeighbors(dfA, key, 2).show()

    val res = trans(model)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: MinHashLSHModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[MinHashLSHServingModel]
    val rowsFeatures = new Array[SRow](3)
    val training = Seq(
      (3, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (5, 1.0)))),
      (4, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (5, 1.0)))),
      (5, Vectors.sparse(6, Seq((1, 1.0), (2, 1.0), (4, 1.0))))
    )
    for (i <- 0 until rowsFeatures.length) {
      rowsFeatures(i) = new SRow(Array(training(i)._2))
    }

    val schema = new StructType().add(new StructField(model.getInputCol, new VectorUDT, true))
    val dataset = new SDFrame(rowsFeatures)(schema)
    transModel.transform(dataset)
  }
}
