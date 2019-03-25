package org.apache.spark.ml.regression

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.feature.PredictionServingModel
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap

class DecisionTreeRegressionServingModel(stage: DecisionTreeRegressionModel)
  extends PredictionServingModel[Vector, DecisionTreeRegressionServingModel] {

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
    transformSchema(dataset.schema)
    transformImpl(dataset)
  }

  override def transformImpl(dataset: SDFrame): SDFrame = {
    val predictUDF = {
      UDF.make[Double, Vector](feature =>
        predict(feature))
    }
    val predictVarianceUDF = {
      UDF.make[Double, Vector](feature =>
        predictVariance(feature))
    }
    if ($(predictionCol).nonEmpty) {
      dataset.withColum(predictUDF.apply(${stage.predictionCol},SCol(${stage.featuresCol})))
    }
    if (isDefined(stage.varianceCol) && $(stage.varianceCol).nonEmpty) {
      dataset.withColum(predictVarianceUDF.apply(${stage.varianceCol}, SCol(${stage.featuresCol})))
    }
     dataset
  }

  override val uid: String = stage.uid
}

object DecisionTreeRegressionServingModel{
  def apply(stage: DecisionTreeRegressionModel): DecisionTreeRegressionServingModel =
    new DecisionTreeRegressionServingModel(stage)
}
