package org.apache.spark.ml.regression

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

object AFTSurvivalRegressionServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val training = spark.createDataFrame(Seq(
      (1.218, 1.0, Vectors.dense(1.560, -0.605)),
      (2.949, 0.0, Vectors.dense(0.346, 2.158)),
      (3.627, 0.0, Vectors.dense(1.380, 0.231)),
      (0.273, 1.0, Vectors.dense(0.520, 1.151)),
      (4.199, 0.0, Vectors.dense(0.795, -0.226))
    )).toDF("label", "censor", "features")
    val quantileProbabilities = Array(0.3, 0.6)
    val aft = new AFTSurvivalRegression()
      .setQuantileProbabilities(quantileProbabilities)
      .setQuantilesCol("quantiles")

    val model = aft.fit(training)

    // Print the coefficients, intercept and scale parameter for AFT survival regression
    println(s"Coefficients: ${model.coefficients}")
    println(s"Intercept: ${model.intercept}")
    println(s"Scale: ${model.scale}")
    model.transform(training).show(false)

    val res = trans(model)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: AFTSurvivalRegressionModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[AFTSurvivalRegressionServingModel]
    val rowsFeatures = new Array[SRow](5)
    val training = Seq(
      (1.0, Vectors.dense(1.560, -0.605)),
      (0.0, Vectors.dense(0.346, 2.158)),
      (0.0, Vectors.dense(1.380, 0.231)),
      (1.0, Vectors.dense(0.520, 1.151)),
      (0.0, Vectors.dense(0.795, -0.226))
    )
    for (i <- 0 until rowsFeatures.length) {
      rowsFeatures(i) = new SRow(Array(training(i)._2))
    }

//    val schema = new StructType().add(new StructField(model.getFeaturesCol, new VectorUDT, true))
    val dataset = transModel.prepareData(rowsFeatures)
    transModel.transform(dataset)
  }
}