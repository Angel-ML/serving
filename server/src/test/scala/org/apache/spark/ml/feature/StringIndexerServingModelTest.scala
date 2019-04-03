package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object StringIndexerServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val df = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df)
    val output = indexed.transform(df)
    output.show()

    val res = trans(indexed)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: StringIndexerModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[StringIndexerServingModel]
    val rowsFeatures = new Array[SRow](5)
    val training = Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    for (i <- 0 until rowsFeatures.length) {
      rowsFeatures(i) = new SRow(Array(training(i)._2))
    }

    val schema = new StructType().add(new StructField(model.getInputCol, StringType, true))
    val dataset = new SDFrame(rowsFeatures)(schema)
    transModel.transform(dataset)
  }
}
