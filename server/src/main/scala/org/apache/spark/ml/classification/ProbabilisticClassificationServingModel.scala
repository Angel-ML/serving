package org.apache.spark.ml.classification

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg._

abstract class ProbabilisticClassificationServingModel[
    FeaturesType, M <: ProbabilisticClassificationServingModel[FeaturesType, M, T],
    T <: ProbabilisticClassificationModel[FeaturesType, T]](stage: T)
  extends ClassificationServingModel[FeaturesType, M, T](stage) with ProbabilisticClassifierParams {

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)

    if (isDefined(stage.thresholds)) {
      require(stage.getThresholds.length == numClasses, this.getClass.getSimpleName +
        ".transform() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${stage.getThresholds.length}")
    }

    // Output selected columns only.
    // This is a bit complicated since it tries to avoid repeated computation.
    var outputData = dataset
    var numColsOutput = 0
    if (stage.getRawPredictionCol.nonEmpty) {
      val predictRawUDF = UDF.make[Vector, Vector](predictRaw, false)
      println(stage.getFeaturesCol)
      outputData = outputData.withColum(predictRawUDF.apply(stage.getRawPredictionCol, SCol(stage.getFeaturesCol)))
      numColsOutput += 1
    }
    if (stage.getProbabilityCol.nonEmpty) {
      val probUDF = if (stage.getRawPredictionCol.nonEmpty) {
        UDF.make[Vector, Vector](raw2probability, false)
          .apply(stage.getProbabilityCol, SCol(stage.getRawPredictionCol))
      } else {
        UDF.make[Vector, Vector](predictProbability, false)
          .apply(stage.getProbabilityCol, SCol(stage.getFeaturesCol))
      }
      outputData = outputData.withColum(probUDF)
      numColsOutput += 1
    }
    if (stage.getPredictionCol.nonEmpty) {
      val predUDF = if (stage.getRawPredictionCol.nonEmpty) {
        UDF.make[Double, Vector](raw2prediction, false)
          .apply(stage.getPredictionCol, SCol(stage.getRawPredictionCol))
      } else if (stage.getProbabilityCol.nonEmpty) {
        UDF.make[Double, Vector](probability2prediction, false)
          .apply(stage.getPredictionCol, SCol(stage.getProbabilityCol))
      } else {
        UDF.make[Double, Vector](predict, false)
          .apply(stage.getPredictionCol, SCol(stage.getFeaturesCol))
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
    if (!isDefined(stage.thresholds)) {
      rawPrediction.argmax
    } else {
      probability2prediction(raw2probability(rawPrediction))
    }
  }

  def raw2probability(rawPrediction: Vector): Vector = {
    val probs = rawPrediction.copy
    raw2probabilityInPlace(probs)
  }

  def predictProbability(features: Vector): Vector = {
    val rawPreds = predictRaw(features)
    raw2probabilityInPlace(rawPreds)
  }

  def probability2prediction(probability: Vector): Double = {
    if (!isDefined(stage.thresholds)) {
      probability.argmax
    } else {
      val thresholds = stage.getThresholds
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
