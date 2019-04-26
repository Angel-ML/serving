package org.apache.spark.ml.tunning

import java.util

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.{VectorUDT, Vectors, Vector}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object TrainValidationSplitServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    // Prepare training and test data.
    val data = spark.read.format("libsvm").load("data/sample_linear_regression_data.txt")
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)

    val lr = new LinearRegression()
      .setMaxIter(10)

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)

    // Run train validation split, and choose the best set of parameters.
    val model = trainValidationSplit.fit(training)

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    model.transform(test)
      .select("features", "label", "prediction")
      .show()

    val res = trans(model)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: TrainValidationSplitModel): SDFrame = {
    val transModel = ModelUtils.transTransformer(model).asInstanceOf[TrainValidationSplitServingModel]
    val rowsFeatures = new Array[SRow](1)
    val size = 10
    val index = Array[Int](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val value = Array[Double](0.4551273600657362, 0.36644694351969087, -0.38256108933468047, -0.4458430198517267,
      0.33109790358914726, 0.8067445293443565, -0.2624341731773887, -0.44850386111659524, -0.07269284838169332, 0.5658035575800715)
//    for (i <- 0 until rowsFeatures.length) {
//      rowsFeatures(i) = new SRow(Array(Vectors.sparse(size, index, value)))
//    }
    val data: util.Map[String, Vector] = new util.HashMap[String, Vector]
    data.put("features", Vectors.sparse(size, index, value))

//    val schema = new StructType().add(new StructField("features", new VectorUDT, true))
    val dataset = transModel.prepareData(data)
    transModel.transform(dataset)
  }
}
