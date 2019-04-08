package org.apache.spark.ml.clustering

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types.{IntegerType, StructType}

class BisectingKMeansServingModel(stage: BisectingKMeansModel)
  extends ServingModel[BisectingKMeansServingModel] with BisectingKMeansParams {

  override def copy(extra: ParamMap): BisectingKMeansServingModel = {
    new BisectingKMeansServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, logging = true)
    val predictUDF = UDF.make[Int, Vector](stage.predict, false)
    dataset.withColum(predictUDF(stage.getPredictionCol, SCol(stage.getFeaturesCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemaImpl(schema)
  }

  /**
    * Validates and transforms the input schema.
    * @param schema input schema
    * @return output schema
    */
  def validateAndTransformSchemaImpl(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, stage.getFeaturesCol, new VectorUDT)
    SchemaUtils.appendColumn(schema, stage.getPredictionCol, IntegerType)
  }

  override val uid: String = stage.uid
}

object BisectingKMeansServingModel {
  def apply(stage: BisectingKMeansModel): BisectingKMeansServingModel =
    new BisectingKMeansServingModel(stage)
}
