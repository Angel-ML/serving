package org.apache.spark.ml.regression

import org.apache.spark.ml.classification.PredictionServingModel
import org.apache.spark.ml.linalg.BLAS.dot
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.LinearRegressionModel

class LinearRegressionServingModel(stage: LinearRegressionModel)
  extends PredictionServingModel[Vector, LinearRegressionServingModel, LinearRegressionModel](stage) {

  override def copy(extra: ParamMap): LinearRegressionServingModel = {
    new LinearRegressionServingModel(stage.copy(extra))
  }

  override def predict(features: Vector): Double = {
    dot(features, stage.coefficients) + stage.intercept
  }

  override val uid: String = stage.uid
}

object LinearRegressionServingModel{
  def apply(stage: LinearRegressionModel): LinearRegressionServingModel =
    new LinearRegressionServingModel(stage)
}
