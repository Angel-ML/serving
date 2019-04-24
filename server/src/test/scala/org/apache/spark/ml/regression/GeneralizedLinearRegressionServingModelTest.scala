package org.apache.spark.ml.regression

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object GeneralizedLinearRegressionServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    // Load training data
    val dataset = spark.read.format("libsvm")
      .load("data/sample_linear_regression_data.txt")


    val glr = new GeneralizedLinearRegression()
      .setFamily("gaussian")
      .setLink("identity")
      .setMaxIter(10)
      .setRegParam(0.3)

    // Fit the model
    val model = glr.fit(dataset)

    // Print the coefficients and intercept for generalized linear regression model
    println(s"Coefficients: ${model.coefficients}")
    println(s"Intercept: ${model.intercept}")

    // Summarize the model over the training set and print out some metrics
    val summary = model.summary
    println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
    println(s"T Values: ${summary.tValues.mkString(",")}")
    println(s"P Values: ${summary.pValues.mkString(",")}")
    println(s"Dispersion: ${summary.dispersion}")
    println(s"Null Deviance: ${summary.nullDeviance}")
    println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
    println(s"Deviance: ${summary.deviance}")
    println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
    println(s"AIC: ${summary.aic}")
    println("Deviance Residuals: ")
    summary.residuals().show()

    val res = trans(model)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: GeneralizedLinearRegressionModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[GeneralizedLinearRegressionServingModel]
    val rowsFeatures = new Array[SRow](1)
    val size = 10
    val index = Array[Int](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val value = Array[Double](0.4551273600657362, 0.36644694351969087, -0.38256108933468047, -0.4458430198517267,
      0.33109790358914726, 0.8067445293443565, -0.2624341731773887, -0.44850386111659524, -0.07269284838169332, 0.5658035575800715)
    for (i <- 0 until rowsFeatures.length) {
      rowsFeatures(i) = new SRow(Array(Vectors.sparse(size, index, value)))
    }

//    val schema = new StructType().add(new StructField(model.getFeaturesCol, new VectorUDT, true))
    val dataset = transModel.prepareData(rowsFeatures)
    transModel.transform(dataset)
  }
}
