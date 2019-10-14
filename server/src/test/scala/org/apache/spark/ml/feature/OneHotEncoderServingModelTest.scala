package org.apache.spark.ml.feature

import java.util

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object OneHotEncoderServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val df = spark.createDataFrame(Seq(
      (0.0, 1.0),
      (1.0, 0.0),
      (2.0, 1.0),
      (0.0, 2.0),
      (0.0, 1.0),
      (2.0, 0.0)
    )).toDF("categoryIndex1", "categoryIndex2")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryVec1", "categoryVec2"))
    val model = encoder.fit(df)

    val encoded = model.transform(df)
    encoded.show()

    model.save("./models/spark/onehot/1")
    val res = trans(model)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(4).toString)
    res.printSchema()
  }

  def trans(model: OneHotEncoderModel): SDFrame = {
    val transModel = ModelUtils.transTransformer(model).asInstanceOf[OneHotEncoderServingModel]
    val rowsFeatures = new Array[SRow](5)
    val training = Seq(
      Array(0.0, 1.0),
      Array(1.0, 0.0),
      Array(2.0, 1.0),
      Array(0.0, 2.0),
      Array(0.0, 1.0),
      Array(2.0, 0.0)
    )
    for (i <- 0 until rowsFeatures.length) {
      rowsFeatures(i) = new SRow(training(i).toArray)
    }
    val data: util.Map[String, Double] = new util.HashMap[String, Double]
    data.put("categoryIndex1", 0.0)
    data.put("categoryIndex2", 1.0)

//    val schema = new StructType().add(new StructField("categoryIndex1", DoubleType, true))
//      .add(new StructField("categoryIndex2", DoubleType, true))
    val dataset = transModel.prepareData(data)
    transModel.transform(dataset)
  }
}
