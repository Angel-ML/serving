package org.apache.spark.ml.classification

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.ml.{PredictionModel, PredictorParams}
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}

abstract class PredictionServingModel[FeaturesType, M <: PredictionServingModel[FeaturesType, M, T],
  T <: PredictionModel[FeaturesType, T]](stage: T)
  extends ServingModel[M]{

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    if (stage.getPredictionCol.nonEmpty) {
      transformImpl(dataset)
    } else {
      this.logWarning(s"$uid: Predictor.transform() was called as NOOP" +
        " since no output columns were set.")
      dataset
    }
  }

  def transformImpl(dataset: SDFrame): SDFrame = {
    val predictUDF = {
      UDF.make[Double, Vector](predict, false)
    }

    dataset.withColum(predictUDF.apply(stage.getPredictionCol,SCol(stage.getFeaturesCol)))
  }

  /**
    * Returns the SQL DataType corresponding to the FeaturesType type parameter.
    *
    * This is used by `validateAndTransformSchema()`.
    * This workaround is needed since SQL has different APIs for Scala and Java.
    *
    * The default value is VectorUDT, but it may be overridden if FeaturesType is not Vector.
    */
  protected def featuresDataType: DataType = new VectorUDT

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemaImpl(schema, false, featuresDataType)
  }

  /**
    * Validates and transforms the input schema with the provided param map.
    *
    * @param schema input schema
    * @param fitting whether this is in fitting
    * @param featuresDataType  SQL DataType for FeaturesType.
    *                          E.g., `VectorUDT` for vector features.
    * @return output schema
    */
  def validateAndTransformSchemaImpl(
                                            schema: StructType,
                                            fitting: Boolean,
                                            featuresDataType: DataType): StructType = {
    // TODO: Support casting Array[Double] and Array[Float] to Vector when FeaturesType = Vector
    SchemaUtils.checkColumnType(schema, stage.getFeaturesCol, featuresDataType)
    if (fitting) {
      SchemaUtils.checkNumericType(schema, stage.getLabelCol)

      this match {
        case p: HasWeightCol =>
          if (isDefined(p.weightCol) && p.getWeightCol.nonEmpty) {
            SchemaUtils.checkNumericType(schema, p.getWeightCol)
          }
        case _ =>
      }
    }
    SchemaUtils.appendColumn(schema, stage.getPredictionCol, DoubleType)
  }

  def predict(features: Vector): Double

}
