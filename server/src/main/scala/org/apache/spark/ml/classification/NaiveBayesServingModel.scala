package org.apache.spark.ml.classification

import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap

class NaiveBayesServingModel(stage: NaiveBayesModel)
  extends ProbabilisticClassificationServingModel[Vector, NaiveBayesServingModel] {

  private val Multinomial: String = "multinomial"

  private val Bernoulli: String = "bernoulli"

  override def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        var i = 0
        val size = dv.size
        val maxLog = dv.values.max
        while (i < size) {
          dv.values(i) = math.exp(dv.values(i) - maxLog)
          i += 1
        }
        val probSum = dv.values.sum
        i = 0
        while (i < size) {
          dv.values(i) = dv.values(i) / probSum
          i += 1
        }
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in NaiveBayesModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  override def predictRaw(features: Vector): Vector = {
    $(stage.modelType) match {
      case Multinomial =>
        multinomialCalculation(features)
      case Bernoulli =>
        bernoulliCalculation(features)
      case _ =>
        // This should never happen.
        throw new UnknownError(s"Invalid modelType: ${$(stage.modelType)}.")
    }
  }

  override def copy(extra: ParamMap): NaiveBayesServingModel = {
    new NaiveBayesServingModel(stage.copy(extra))
  }

  override val uid: String = stage.uid

  private def multinomialCalculation(features: Vector) = {
    val prob = stage.theta.multiply(features)
    BLAS.axpy(1.0, stage.pi, prob)
    prob
  }

  private def bernoulliCalculation(features: Vector) = {
    features.foreachActive((_, value) =>
      require(value == 0.0 || value == 1.0,
        s"Bernoulli naive Bayes requires 0 or 1 feature values but found $features.")
    )
    val prob = thetaMinusNegTheta.get.multiply(features)
    BLAS.axpy(1.0, stage.pi, prob)
    BLAS.axpy(1.0, negThetaSum.get, prob)
    prob
  }

  /**
    * Bernoulli scoring requires log(condprob) if 1, log(1-condprob) if 0.
    * This precomputes log(1.0 - exp(theta)) and its sum which are used for the linear algebra
    * application of this condition (in predict function).
    */
  private lazy val (thetaMinusNegTheta, negThetaSum) = $(stage.modelType) match {
    case Multinomial => (None, None)
    case Bernoulli =>
      val negTheta = stage.theta.map(value => math.log(1.0 - math.exp(value)))
      val ones = new DenseVector(Array.fill(stage.theta.numCols) {1.0})
      val thetaMinusNegTheta = stage.theta.map { value =>
        value - math.log(1.0 - math.exp(value))
      }
      (Option(thetaMinusNegTheta), Option(negTheta.multiply(ones)))
    case _ =>
      // This should never happen.
      throw new UnknownError(s"Invalid modelType: ${$(stage.modelType)}.")
  }

  override def numClasses: Int = stage.numClasses
}

object NaiveBayesServingModel {
  def apply(stage: NaiveBayesModel): NaiveBayesServingModel = new NaiveBayesServingModel(stage)
}