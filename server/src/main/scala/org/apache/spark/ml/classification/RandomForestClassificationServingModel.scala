package org.apache.spark.ml.classification

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap

class RandomForestClassificationServingModel(stage: RandomForestClassificationModel)
  extends ProbabilisticClassificationServingModel[Vector, RandomForestClassificationServingModel] {

    override def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
      rawPrediction match {
        case dv: DenseVector =>
          ProbabilisticClassificationModel.normalizeToProbabilitiesInPlace(dv)
          dv
        case sv: SparseVector =>
          throw new RuntimeException("Unexpected error in RandomForestClassificationModel:" +
            " raw2probabilityInPlace encountered SparseVector")
      }
    }

    override def predictRaw(features: Vector): Vector = {
      val votes = Array.fill[Double](stage.numClasses)(0.0)
      stage.trees.view.foreach { tree =>
        val classCounts: Array[Double] = tree.rootNode.predictImpl(features).impurityStats.stats
        val total = classCounts.sum
        if (total != 0) {
          var i = 0
          while (i < stage.numClasses) {
            votes(i) += classCounts(i) / total
            i += 1
          }
        }
      }
      Vectors.dense(votes)
    }

    override def transformImpl(dataset: SDFrame): SDFrame = {
      val predictUDF = UDF.make[Double, Any](features => predict(features.asInstanceOf[Vector]))
      dataset.withColum(predictUDF.apply($(stage.predictionCol), SCol($(stage.featuresCol))))
    }

    override def copy(extra: ParamMap): RandomForestClassificationServingModel = {
      new RandomForestClassificationServingModel(stage.copy(extra))
    }

    override val uid: String = stage.uid

    override def numClasses: Int = stage.numClasses
  }

object RandomForestClassificationServingModel {
  def apply(stage: RandomForestClassificationModel): RandomForestClassificationServingModel =
    new RandomForestClassificationServingModel(stage)
}
