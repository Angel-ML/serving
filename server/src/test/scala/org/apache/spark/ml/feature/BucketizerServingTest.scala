package org.apache.spark.ml.feature

import java.util

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

object BucketizerServingTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    println(dataFrame.head())

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(dataFrame)

    println(s"Bucketizer output with ${bucketizer.getSplits.length-1} buckets")
    bucketedData.show()
    bucketizer.save("f:/model/bucketizer")

    val res = trans(bucketizer)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: Bucketizer): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[BucketizerServing]
    val rowsFeatures = new Array[SRow](5)
    val training = Array(1000, -20, 0.1, 5, 200)
//    for (i <- 0 until rowsFeatures.length) {
//      rowsFeatures(i) = new SRow(Array(training(i)))
//    }
    val data: util.Map[String, Double] = new util.HashMap[String, Double]
    data.put(model.getInputCol, 1)

//    val schema = new StructType().add(new StructField(model.getInputCol, DoubleType, true))
    val dataset = transModel.prepareData(data)
    transModel.transform(dataset)
  }
}
