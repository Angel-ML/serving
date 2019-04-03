package org.apache.spark.ml.classification

import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap

class MultilayerPerceptronClassificationServingModel(stage: MultilayerPerceptronClassificationModel)
  extends ProbabilisticClassificationServingModel[Vector, MultilayerPerceptronClassificationServingModel,
    MultilayerPerceptronClassificationModel](stage) with MultilayerPerceptronParams {

  override def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    stage.mlpModel.raw2ProbabilityInPlace(rawPrediction)
  }

  override def predictRaw(features: Vector): Vector = {
    stage.mlpModel.predictRaw(features)
  }

  override def numClasses: Int = stage.layers.last

  override def copy(extra: ParamMap): MultilayerPerceptronClassificationServingModel = {
    new MultilayerPerceptronClassificationServingModel(stage.copy(extra))
  }

  override val uid: String = stage.uid

  /**
    * Predict label for the given features.
    * This internal method is used to implement `transform()` and output [[predictionCol]].
    */
  override def predict(features: Vector): Double = {
    LabelConverter.decodeLabel(stage.mlpModel.predict(features))
  }
}

object MultilayerPerceptronClassificationServingModel {
  def apply(stage: MultilayerPerceptronClassificationModel): MultilayerPerceptronClassificationServingModel =
    new MultilayerPerceptronClassificationServingModel(stage)
}
