package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object FeatureHasherServingTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val df = spark.createDataFrame(Seq(
      (2.0, true, "1", "foo"),
      (3.0, false, "2", "bar")
    )).toDF("real", "bool", "stringNum", "string")

    val hasher = new FeatureHasher()
      .setInputCols("real", "bool", "stringNum", "string")
      .setOutputCol("features")

    hasher.transform(df).show(false)

    val res = trans(hasher)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: FeatureHasher): SDFrame = {
    val transModel = ModelUtils.transTransformer(model).asInstanceOf[FeatureHasherServing]
    val rowsFeatures = new Array[SRow](1)
    val training = Seq(
      Array(0.0, true, "1", "foo")
    )
    for (i <- 0 until rowsFeatures.length) {
      rowsFeatures(i) = new SRow(training(i))
    }

    val schema = new StructType().add(new StructField("real", DoubleType, true))
      .add(new StructField("bool", BooleanType, true))
      .add(new StructField("stringNum", StringType, true))
      .add(new StructField("string", StringType, true))
    val dataset = new SDFrame(rowsFeatures)(schema)
    transModel.transform(dataset)
  }
}
