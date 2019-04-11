package org.apache.spark.ml.regression

import org.apache.spark.ml.classification.PredictionServingModel
import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree.DecisionTreeRegressorParams

class DecisionTreeRegressionServingModel(stage: DecisionTreeRegressionModel)
  extends PredictionServingModel[Vector, DecisionTreeRegressionServingModel, DecisionTreeRegressionModel](stage)
    with DecisionTreeRegressorParams {

  override def copy(extra: ParamMap): DecisionTreeRegressionServingModel = {
    new DecisionTreeRegressionServingModel(stage.copy(extra))
  }

  override def predict(features: Vector): Double = {
    stage.rootNode.predictImpl(features).prediction
  }

  def predictVariance(features: Vector): Double = {
    stage.rootNode.predictImpl(features).impurityStats.calculate()
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    transformImpl(dataset)
  }

  override def transformImpl(dataset: SDFrame): SDFrame = {
    val predictUDF = {
      UDF.make[Double, Vector](predict, false)
    }
    val predictVarianceUDF = {
      UDF.make[Double, Vector](predictVariance, false)
    }
    var output = dataset
    if (stage.getPredictionCol.nonEmpty) {
      output = dataset.withColum(predictUDF.apply(stage.getPredictionCol,SCol(stage.getFeaturesCol)))
    }
    if (isDefined(stage.varianceCol) && stage.getVarianceCol.nonEmpty) {
      output = dataset.withColum(predictVarianceUDF.apply(stage.getVarianceCol, SCol(stage.getFeaturesCol)))
    }
     output
  }

  override val uid: String = stage.uid
}

object DecisionTreeRegressionServingModel{
  def apply(stage: DecisionTreeRegressionModel): DecisionTreeRegressionServingModel =
    new DecisionTreeRegressionServingModel(stage)
}
