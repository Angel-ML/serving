package org.apache.spark.ml.regression

import org.apache.spark.ml.classification.PredictionServingModel
import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap

class RandomForestRegressionServingModel(stage: RandomForestRegressionModel)
  extends PredictionServingModel[Vector, RandomForestRegressionServingModel, RandomForestRegressionModel](stage) {

  override def copy(extra: ParamMap): RandomForestRegressionServingModel = {
    new RandomForestRegressionServingModel(stage.copy(extra))
  }

  override def predict(features: Vector): Double ={
    stage.trees.map(_.rootNode.predictImpl(features).prediction).sum / stage.getNumTrees
  }

  override def transformImpl(dataset: SDFrame): SDFrame = {
    val predictUDF = UDF.make[Double, Vector](predict, false)
    dataset.withColum(predictUDF.apply(stage.getPredictionCol, SCol(stage.getFeaturesCol)))
  }

  override val uid: String = stage.uid
}

object RandomForestRegressionServingModel{
  def apply(stage: RandomForestRegressionModel): RandomForestRegressionServingModel =
    new RandomForestRegressionServingModel(stage)
}
