package org.apache.spark.ml.classification
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap

class LogisticRegressionServingModel(stage: LogisticRegressionModel)
  extends ProbabilisticClassificationServingModel[Vector, LogisticRegressionServingModel] {

  private val isMultinomial: Boolean = if (stage.numClasses > 2) true else false

  override def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        if (isMultinomial) {
          val size = dv.size
          val values = dv.values

          // get the maximum margin
          val maxMarginIndex = rawPrediction.argmax
          val maxMargin = rawPrediction(maxMarginIndex)

          if (maxMargin == Double.PositiveInfinity) {
            var k = 0
            while (k < size) {
              values(k) = if (k == maxMarginIndex) 1.0 else 0.0
              k += 1
            }
          } else {
            val sum = {
              var temp = 0.0
              var k = 0
              while (k < numClasses) {
                values(k) = if (maxMargin > 0) {
                  math.exp(values(k) - maxMargin)
                } else {
                  math.exp(values(k))
                }
                temp += values(k)
                k += 1
              }
              temp
            }
            BLAS.scal(1 / sum, dv)
          }
          dv
        } else {
          var i = 0
          val size = dv.size
          while (i < size) {
            dv.values(i) = 1.0 / (1.0 + math.exp(-dv.values(i)))
            i += 1
          }
          dv
        }
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in LogisticRegressionModel:" +
          " raw2probabilitiesInPlace encountered SparseVector")
    }
  }

  override def predictRaw(features: Vector): Vector = {
    if (isMultinomial) {
      margins(features)
    } else {
      val m = margin(features)
      Vectors.dense(-m, m)
    }
  }

  override def numClasses: Int = stage.numClasses

  override def copy(extra: ParamMap): LogisticRegressionServingModel = {
    new LogisticRegressionServingModel(stage.copy(extra))
  }

  override def predict(features: Vector): Double = if (isMultinomial) {
    super.predict(features)
  } else {
    // Note: We should use getThreshold instead of $(threshold) since getThreshold is overridden.
    if (score(features) > stage.getThreshold) 1 else 0
  }

  override def raw2prediction(rawPrediction: Vector): Double = {
    if (isMultinomial) {
      super.raw2prediction(rawPrediction)
    } else {
      // Note: We should use getThreshold instead of $(threshold) since getThreshold is overridden.
      val t = stage.getThreshold
      val rawThreshold = if (t == 0.0) {
        Double.NegativeInfinity
      } else if (t == 1.0) {
        Double.PositiveInfinity
      } else {
        math.log(t / (1.0 - t))
      }
      if (rawPrediction(1) > rawThreshold) 1 else 0
    }
  }

  override def probability2prediction(probability: Vector): Double = {
    if (isMultinomial) {
      super.probability2prediction(probability)
    } else {
      // Note: We should use getThreshold instead of $(threshold) since getThreshold is overridden.
      if (probability(1) > stage.getThreshold) 1 else 0
    }
  }

  override val uid: String = stage.uid

  /** Margin (rawPrediction) for class label 1.  For binary classification only. */
  private val margin: Vector => Double = (features) => {
    BLAS.dot(features, stage.coefficients) + stage.intercept
  }

  /** Margin (rawPrediction) for each class label. */
  private val margins: Vector => Vector = (features) => {
    val m = stage.interceptVector.toDense.copy
    BLAS.gemv(1.0, stage.coefficientMatrix, features, 1.0, m)
    m
  }

  /** Score (probability) for class label 1.  For binary classification only. */
  private val score: Vector => Double = (features) => {
    val m = margin(features)
    1.0 / (1.0 + math.exp(-m))
  }
}

object LogisticRegressionServingModel {
  def apply(stage: LogisticRegressionModel): LogisticRegressionServingModel =
    new LogisticRegressionServingModel(stage)
}