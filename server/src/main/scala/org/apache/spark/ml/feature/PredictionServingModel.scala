package org.apache.spark.ml.feature

import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.{DataType, StructType}

abstract class PredictionServingModel[FeaturesType, M <: PredictionServingModel[FeaturesType, M]]
  extends ServingModel[M] with PredictorParams{

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    if ($(predictionCol).nonEmpty) {
      transformImpl(dataset)
    } else {
      this.logWarning(s"$uid: Predictor.transform() was called as NOOP" +
        " since no output columns were set.")
      dataset
    }
  }

  def transformImpl(dataset: SDFrame): SDFrame = {
    val predictUDF = {
      UDF.make[Double, Any](feature =>
        predict(feature.asInstanceOf[FeaturesType]))
    }

    dataset.withColum(predictUDF.apply($(predictionCol),SCol($(featuresCol))))
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
    validateAndTransformSchema(schema, false, featuresDataType)
  }

  def predict(features: FeaturesType): Double

}
