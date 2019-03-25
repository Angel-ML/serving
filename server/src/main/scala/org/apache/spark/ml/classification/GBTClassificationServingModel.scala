package org.apache.spark.ml.classification

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml.classification.GBTClassificationModel
import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.tree.loss.{LogLoss => OldLogLoss}

class GBTClassificationServingModel(stage: GBTClassificationModel)
  extends ProbabilisticClassificationServingModel[Vector, GBTClassificationServingModel] {

  override def copy(extra: ParamMap): GBTClassificationServingModel = {
    new GBTClassificationServingModel(stage.copy(extra))
  }

  override def transformImpl(dataset: SDFrame): SDFrame = {
    val predictUDF = UDF.make[Double, Any](features => predict(features.asInstanceOf[Vector]))
    dataset.withColum(predictUDF.apply($(stage.predictionCol), SCol($(stage.featuresCol))))
  }

  override def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        dv.values(0) = loss.computeProbability(dv.values(0))
        dv.values(1) = 1.0 - dv.values(0)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in GBTClassificationModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  override def predictRaw(features: Vector): Vector = {
    val prediction: Double = margin(features)
    Vectors.dense(Array(-prediction, prediction))
  }

  override def predict(features: Vector): Double = {
    if (isDefined(stage.thresholds)) {
      super.predict(features)
    } else {
      if (margin(features) > 0.0) 1.0 else 0.0
    }
  }

  def margin(features: Vector): Double = {
    val treePredictions = stage.trees.map(_.rootNode.predictImpl(features).prediction)
    blas.ddot(stage.numTrees, treePredictions, 1, stage.treeWeights, 1)
  }

  override val uid: String = stage.uid

  val loss = stage.getLossType match {
    case "logistic" => OldLogLoss
    case _ =>
      // Should never happen because of check in setter method.
      throw new RuntimeException(s"GBTClassifier was given bad loss type: ${stage.getLossType}")
  }

  override def numClasses: Int = stage.numClasses
}

object GBTClassificationServingModel{
  def apply(stage: GBTClassificationModel): GBTClassificationServingModel =
    new GBTClassificationServingModel(stage)
}
