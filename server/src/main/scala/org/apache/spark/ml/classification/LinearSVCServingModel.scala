package org.apache.spark.ml.classification
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap

class LinearSVCServingModel(stage: LinearSVCModel)
  extends ClassificationServingModel[Vector, LinearSVCServingModel, LinearSVCModel](stage) with LinearSVCParams {

  override def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(-m, m)
  }

  override def numClasses: Int = 2

  override def copy(extra: ParamMap): LinearSVCServingModel = {
    new LinearSVCServingModel(stage.copy(extra))
  }

  override val uid: String = stage.uid

  private val margin: Vector => Double = (features) => {
    BLAS.dot(features, stage.coefficients) + stage.intercept
  }

  override def predict(features: Vector): Double ={
    if (margin(features) > stage.getThreshold) 1.0 else 0.0
  }

  override def raw2prediction(rawPrediction: Vector): Double = {
    if (rawPrediction(1) > stage.getThreshold) 1.0 else 0.0
  }
}

object LinearSVCServingModel {
  def apply(stage: LinearSVCModel): LinearSVCServingModel = new LinearSVCServingModel(stage)
}