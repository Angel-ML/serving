package org.apache.spark.ml.regression

import java.util

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object LinearRegressionServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    // Load training data
    val training = spark.read.format("libsvm")
      .load("data/sample_linear_regression_data.txt")

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    val res = trans(lrModel)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: LinearRegressionModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[LinearRegressionServingModel]
    val rowsFeatures = new Array[SRow](1)
    val size = 10
    val index = Array[Int](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val value = Array[Double](0.4551273600657362, 0.36644694351969087, -0.38256108933468047, -0.4458430198517267,
      0.33109790358914726, 0.8067445293443565, -0.2624341731773887, -0.44850386111659524, -0.07269284838169332, 0.5658035575800715)
//    for (i <- 0 until rowsFeatures.length) {
//      rowsFeatures(i) = new SRow(Array(Vectors.sparse(size, index, value)))
//    }
    val data: util.Map[String, Vector] = new util.HashMap[String, Vector]
    data.put(model.getFeaturesCol, Vectors.sparse(size, index, value))

//    val schema = new StructType().add(new StructField(model.getFeaturesCol, new VectorUDT, true))
    val dataset = transModel.prepareData(data)
    transModel.transform(dataset)
  }
}
