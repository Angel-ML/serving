package org.apache.spark.ml.regression

import java.util.Arrays.binarySearch

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.IsotonicRegressionModel
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.{DoubleType, StructType}

class IsotonicRegressionServingModel(stage: IsotonicRegressionModel)
  extends ServingModel[IsotonicRegressionServingModel] with IsotonicRegressionBase {

  override def copy(extra: ParamMap): IsotonicRegressionServingModel = {
    new IsotonicRegressionServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    val predictUDF = dataset.schema($(stage.featuresCol)).dataType match {
      case DoubleType =>
        UDF.make[Double, Double](feature => predict(feature))
      case _: VectorUDT =>
        val idx = $(stage.featureIndex)
        UDF.make[Double, Vector](feature => predict(feature(idx)))
    }
    dataset.withColum(predictUDF.apply($(stage.predictionCol), SCol($(stage.featuresCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, false)
  }

  override val uid: String = stage.uid

  def predict(testData: Double): Double = {
    def linearInterpolation(x1: Double, y1: Double, x2: Double, y2: Double, x: Double): Double = {
      y1 + (y2 - y1) * (x - x1) / (x2 - x1)
    }

    val boundaries = stage.boundaries.toArray
    val predictions = stage.predictions.toArray

    val foundIndex = binarySearch(boundaries, testData)
    val insertIndex = -foundIndex - 1

    // Find if the index was lower than all values,
    // higher than all values, in between two values or exact match.
    if (insertIndex == 0) {
      predictions.head
    } else if (insertIndex == boundaries.length) {
      predictions.last
    } else if (foundIndex < 0) {
      linearInterpolation(
        boundaries(insertIndex - 1),
        predictions(insertIndex - 1),
        boundaries(insertIndex),
        predictions(insertIndex),
        testData)
    } else {
      predictions(foundIndex)
    }
  }
}

object IsotonicRegressionServingModel {
  def apply(stage: IsotonicRegressionModel): IsotonicRegressionServingModel = new IsotonicRegressionServingModel(stage)
}
