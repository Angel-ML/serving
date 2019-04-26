package org.apache.spark.ml.classification

import java.util

import org.apache.spark.ml.data.{SCol, SDFrame, SRow, UDF}
import org.apache.spark.ml.linalg.{VectorUDT, _}
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.ml.{PredictionModel, PredictorParams}
import org.apache.spark.sql.types._

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

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.featuresCol)) {
      val schema = new StructType().add(new StructField(stage.getFeaturesCol, new VectorUDT, true))
      new SDFrame(rows)(schema)
    } else {
      throw new Exception (s"featuresCol of ${stage} is not defined!")
    }
  }

  override def prepareData(feature: util.Map[String, _]): SDFrame = {
    if (stage.isDefined(stage.featuresCol)) {
      val featureName = feature.keySet.toArray
      if (!featureName.contains(stage.getFeaturesCol)) {
        throw new Exception (s"the ${stage.getFeaturesCol} is not included in the input col(s)")
      } else if (!feature.get(stage.getFeaturesCol).isInstanceOf[Vector]) {
        throw new Exception (s"the type of col ${stage.getFeaturesCol} is not Vector")
      } else {
        val schema = new StructType().add(new StructField(stage.getFeaturesCol, new VectorUDT, true))
        val rows =  Array(new SRow(Array(feature.get(stage.getFeaturesCol))))
        new SDFrame(rows)(schema)
      }
    } else {
      throw new Exception (s"featuresCol of ${stage} is not defined!")
    }
  }
}
