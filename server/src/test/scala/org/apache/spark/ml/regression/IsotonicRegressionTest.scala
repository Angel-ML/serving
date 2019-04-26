package org.apache.spark.ml.regression

import java.util

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

object IsotonicRegressionTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val data = spark.read.format("libsvm").load("data/sample_isotonic_regression_libsvm_data.txt")

    val ir = new IsotonicRegression()
    val model = ir.fit(data)

    val resdataset = trans(model)
    println(resdataset.schema, resdataset.columns.length, resdataset.columns(0),
      resdataset.getRow(0).get(0).toString, resdataset.getRow(0).get(1).toString)
    resdataset.printSchema()

    println(s"Boundaries in increasing order: ${model.boundaries}\n")
    println(s"Predictions associated with the boundaries: ${model.predictions}\n")

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = model.transform(data)
    indexedData.show()

    spark.stop()
  }

  def trans(model: IsotonicRegressionModel): SDFrame = {
    val transModel = ModelUtils.transModel(model)//.asInstanceOf[IsotonicRegressionServingModel]
    val rows = new Array[SRow](5)
    val x = 0.0
    for (i <- 0 until rows.length) {
      rows(i) = new SRow(Array(x + 0.01))
    }
    val data: util.Map[String, Double] = new util.HashMap[String, Double]
    data.put(model.getFeaturesCol, 0.1)
//    val schema = new StructType().add(new StructField(model.getFeaturesCol, DoubleType, true))
    val dataset = transModel.prepareData(data)
    transModel.transform(dataset)
  }
}
