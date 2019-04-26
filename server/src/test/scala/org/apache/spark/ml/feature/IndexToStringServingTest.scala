package org.apache.spark.ml.feature

import java.util

import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object IndexToStringServingTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    println(s"Transformed string column '${indexer.getInputCol}' " +
      s"to indexed column '${indexer.getOutputCol}'")
    indexed.show()

    val inputColSchema = indexed.schema(indexer.getOutputCol)
    println(s"StringIndexer will store labels in output column metadata: " +
      s"${Attribute.fromStructField(inputColSchema).toString}\n")

    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")
      .setLabels(indexer.labels)

    val converted = converter.transform(indexed)

    println(s"Transformed indexed column '${converter.getInputCol}' back to original string " +
      s"column '${converter.getOutputCol}' using labels in metadata")
    converted.select("id", "categoryIndex", "originalCategory").show()
//    println(converter.getLabels)

    val res = trans(converter)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: IndexToString): SDFrame = {
    val transModel = ModelUtils.transTransformer(model).asInstanceOf[IndexToStringServing]
    val rowsFeatures = new Array[SRow](2)
    val training = Seq(
      Array[Any](0.0),
      Array[Any](0.0)
    )
//    for (i <- 0 until rowsFeatures.length) {
//      rowsFeatures(i) = new SRow(training(i))
//    }
    val data: util.Map[String, Double] = new util.HashMap[String, Double]
    data.put(model.getInputCol, 0.0)

//    val schema = new StructType().add(new StructField(model.getInputCol, DoubleType, true))
    val dataset = transModel.prepareData(data)
    transModel.transform(dataset)
  }
}
