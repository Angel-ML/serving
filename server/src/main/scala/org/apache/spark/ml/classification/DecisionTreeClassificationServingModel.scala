package org.apache.spark.ml.classification
import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree.DecisionTreeClassifierParams

class DecisionTreeClassificationServingModel(stage: DecisionTreeClassificationModel)
  extends ProbabilisticClassificationServingModel[Vector, DecisionTreeClassificationServingModel,
    DecisionTreeClassificationModel](stage) with DecisionTreeClassifierParams {

  override def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        ProbabilisticClassificationModel.normalizeToProbabilitiesInPlace(dv)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in DecisionTreeClassificationModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  override def predictRaw(features: Vector): Vector = {
    Vectors.dense(stage.rootNode.predictImpl(features).impurityStats.stats.clone())
  }

  override def numClasses: Int = stage.numClasses

  override def copy(extra: ParamMap): DecisionTreeClassificationServingModel = {
    new DecisionTreeClassificationServingModel(stage.copy(extra))
  }

  override val uid: String = stage.uid

  override def predict(features: Vector): Double = {
    stage.rootNode.predictImpl(features).prediction
  }
}

object DecisionTreeClassificationServingModel {
  def apply(stage: DecisionTreeClassificationModel): DecisionTreeClassificationServingModel =
    new DecisionTreeClassificationServingModel(stage)
}