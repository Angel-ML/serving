package org.apache.spark.ml.regression

import java.util.Arrays.binarySearch

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types.{DoubleType, StructType}

class IsotonicRegressionServingModel(stage: IsotonicRegressionModel)
  extends ServingModel[IsotonicRegressionServingModel] {

  override def copy(extra: ParamMap): IsotonicRegressionServingModel = {
    new IsotonicRegressionServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    val predictUDF = dataset.schema(stage.getFeaturesCol).dataType match {
      case DoubleType =>
        UDF.make[Double, Double](predict, false)
      case _: VectorUDT =>
        val idx = stage.getFeatureIndex
        UDF.make[Double, Vector](feature => predict(feature(idx)), false)
    }
    dataset.withColum(predictUDF.apply(stage.getPredictionCol, SCol(stage.getFeaturesCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemaImpl(schema, false)
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

  def validateAndTransformSchemaImpl(
                                schema: StructType,
                                fitting: Boolean): StructType = {
    if (fitting) {
      SchemaUtils.checkNumericType(schema, stage.getLabelCol)
      if (stage.hasWeightCol) {
        SchemaUtils.checkNumericType(schema, stage.getWeightCol)
      } else {
        logInfo("The weight column is not defined. Treat all instance weights as 1.0.")
      }
    }

    val featuresType = schema(stage.getFeaturesCol).dataType
    require(featuresType == DoubleType || featuresType.isInstanceOf[VectorUDT])
    SchemaUtils.appendColumn(schema, stage.getPredictionCol, DoubleType)
  }

}

object IsotonicRegressionServingModel {
  def apply(stage: IsotonicRegressionModel): IsotonicRegressionServingModel =
    new IsotonicRegressionServingModel(stage)
}
