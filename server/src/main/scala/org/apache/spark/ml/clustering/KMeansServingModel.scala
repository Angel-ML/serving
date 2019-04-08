package org.apache.spark.ml.clustering

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types.{IntegerType, StructType}

class KMeansServingModel(stage: KMeansModel) extends ServingModel[KMeansServingModel] {

  override def copy(extra: ParamMap): KMeansServingModel = {
    new KMeansServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)

    val predictUDF = UDF.make[Int, Vector](stage.predict, false)
    dataset.withColum(predictUDF.apply(stage.getPredictionCol, SCol(stage.getFeaturesCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemaImpl(schema)
  }

  def validateAndTransformSchemaImpl(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, stage.getFeaturesCol, new VectorUDT)
    SchemaUtils.appendColumn(schema, stage.getPredictionCol, IntegerType)
  }

  override val uid: String = stage.uid
}

object KMeansServingModel {
  def apply(stage: KMeansModel): KMeansServingModel = new KMeansServingModel(stage)
}