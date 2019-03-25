package org.apache.spark.ml.clustering

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.stat.distribution.MultivariateGaussian
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.StructType

class GaussianMixtureServingModel(stage: GaussianMixtureModel) extends ServingModel[GaussianMixtureServingModel] {

  override def copy(extra: ParamMap): GaussianMixtureServingModel = {
    new GaussianMixtureServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    val predUDF = UDF.make[Int, Vector](features => predict(features))
    val probUDF = UDF.make[Vector, Vector](features => predictProbability(features))
    dataset.withColum(predUDF.apply(${stage.predictionCol}, SCol(${stage.featuresCol})))
      .withColum(probUDF.apply(${stage.probabilityCol}, SCol(${stage.featuresCol})))
  }

  override def transformSchema(schema: StructType): StructType = ???

  override val uid: String = stage.uid

  private def predict(features: Vector): Int = {
    val r = predictProbability(features)
    r.argmax
  }

  private def predictProbability(features: Vector): Vector = {
    val probs: Array[Double] =
      GaussianMixtureServingModel.computeProbabilities(features.asBreeze.toDenseVector, stage.gaussians, stage.weights)
    Vectors.dense(probs)
  }
}

object GaussianMixtureServingModel {

  def apply(stage: GaussianMixtureModel): GaussianMixtureServingModel = new GaussianMixtureServingModel(stage)

  private def computeProbabilities(
                            features: BDV[Double],
                            dists: Array[MultivariateGaussian],
                            weights: Array[Double]): Array[Double] = {
    val p = weights.zip(dists).map {
      case (weight, dist) => EPSILON + weight * dist.pdf(features)
    }
    val pSum = p.sum
    var i = 0
    while (i < weights.length) {
      p(i) /= pSum
      i += 1
    }
    p
  }

  lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }
}