package org.apache.spark.ml.classification

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg._

abstract class ProbabilisticClassificationServingModel[
    FeaturesType, M <: ProbabilisticClassificationServingModel[FeaturesType, M]]
  extends ClassificationServingModel[FeaturesType, M] with ProbabilisticClassifierParams {

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)

    if (isDefined(thresholds)) {
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".transform() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    // Output selected columns only.
    // This is a bit complicated since it tries to avoid repeated computation.
    var outputData = dataset
    var numColsOutput = 0
    if ($(rawPredictionCol).nonEmpty) {
      val predictRawUDF = UDF.make[Vector, Any](features =>
        predictRaw(features.asInstanceOf[FeaturesType])
      )
      outputData = outputData.withColum(predictRawUDF.apply(getRawPredictionCol, SCol(getFeaturesCol)))
      numColsOutput += 1
    }
    if ($(probabilityCol).nonEmpty) {
      val probUDF = if ($(rawPredictionCol).nonEmpty) {
        UDF.make[Vector, Vector](features =>
          raw2probability(features))
          .apply($(probabilityCol), SCol($(rawPredictionCol)))
      } else {
        UDF.make[Vector, Any](features =>
          predictProbability(features.asInstanceOf[FeaturesType]))
          .apply($(probabilityCol), SCol($(featuresCol)))
      }
      outputData = outputData.withColum(probUDF)
      numColsOutput += 1
    }
    if ($(predictionCol).nonEmpty) {
      val predUDF = if ($(rawPredictionCol).nonEmpty) {
        UDF.make[Double, Vector](features =>
          raw2prediction(features))
          .apply($(predictionCol), SCol($(rawPredictionCol)))
      } else if ($(probabilityCol).nonEmpty) {
        UDF.make[Double, Vector](features =>
          probability2prediction(features))
          .apply($(predictionCol), SCol($(probabilityCol)))
      } else {
        UDF.make[Double, Any](features =>
          predict(features.asInstanceOf[FeaturesType]))
          .apply($(predictionCol), SCol($(featuresCol)))
      }
      outputData = outputData.withColum(predUDF)
      numColsOutput += 1
    }

    if (numColsOutput == 0) {
      this.logWarning(s"$uid: ProbabilisticClassificationModel.transform() was called as NOOP" +
        " since no output columns were set.")
    }
    outputData
  }

  override def raw2prediction(rawPrediction: Vector): Double = {
    if (!isDefined(thresholds)) {
      rawPrediction.argmax
    } else {
      probability2prediction(raw2probability(rawPrediction))
    }
  }

  def raw2probability(rawPrediction: Vector): Vector = {
    val probs = rawPrediction.copy
    raw2probabilityInPlace(probs)
  }

  def predictProbability(features: FeaturesType): Vector = {
    val rawPreds = predictRaw(features)
    raw2probabilityInPlace(rawPreds)
  }

  def probability2prediction(probability: Vector): Double = {
    if (!isDefined(thresholds)) {
      probability.argmax
    } else {
      val thresholds = getThresholds
      var argMax = 0
      var max = Double.NegativeInfinity
      var i = 0
      val probabilitySize = probability.size
      while (i < probabilitySize) {
        // Thresholds are all > 0, excepting that at most one may be 0.
        // The single class whose threshold is 0, if any, will always be predicted
        // ('scaled' = +Infinity). However in the case that this class also has
        // 0 probability, the class will not be selected ('scaled' is NaN).
        val scaled = probability(i) / thresholds(i)
        if (scaled > max) {
          max = scaled
          argMax = i
        }
        i += 1
      }
      argMax
    }
  }

  def raw2probabilityInPlace(rawPrediction: Vector): Vector
}
