package org.apache.spark.ml.transformer

import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.types.StructType


abstract class ServingStage extends Params with Logging {
  def transformSchema(schema: StructType): StructType

  protected def transformSchema(schema: StructType, logging: Boolean): StructType = {
    if (logging) {
      logDebug(s"Input schema: ${schema.json}")
    }
    val outputSchema = transformSchema(schema)
    if (logging) {
      logDebug(s"Expected output schema: ${outputSchema.json}")
    }
    outputSchema
  }

  override def copy(extra: ParamMap): ServingStage
}
