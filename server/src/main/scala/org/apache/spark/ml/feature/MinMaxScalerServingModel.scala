package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types.{StructField, StructType}

class MinMaxScalerServingModel(stage: MinMaxScalerModel)
  extends ServingModel[MinMaxScalerServingModel] {

  override def copy(extra: ParamMap): MinMaxScalerServingModel = {
    new MinMaxScalerServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    val originalRange = (stage.originalMax.asBreeze - stage.originalMin.asBreeze).toArray
    val minArray = stage.originalMin.toArray
    val reScaleUDF = UDF.make[Vector, Vector](feature => {
      val scale = stage.getMax - stage.getMin
      val values = feature.toArray
      val size = feature.size
      var i = 0
      while (i < size) {
        if (!values(i).isNaN) {
          val raw = if (originalRange(i) != 0) (values(i) - minArray(i)) / originalRange(i) else 0.5
          values(i) = raw * scale + stage.getMin
        }
        i += 1
      }
      Vectors.dense(values)
    })
    dataset.withColum(reScaleUDF.apply(stage.getOutputCol, SCol(stage.getInputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemaImpl(schema)
  }

  def validateAndTransformSchemaImpl(schema: StructType): StructType = {
    require(stage.getMin < stage.getMax, s"The specified min(${stage.getMin}) is larger or equal to max(${stage.getMax})")
    SchemaUtils.checkColumnType(schema, stage.getInputCol, new VectorUDT)
    require(!schema.fieldNames.contains(stage.getOutputCol),
      s"Output column ${stage.getOutputCol} already exists.")
    val outputFields = schema.fields :+ StructField(stage.getOutputCol, new VectorUDT, false)
    StructType(outputFields)
  }

  override val uid: String = stage.uid
}

object MinMaxScalerServingModel {
  def apply(stage: MinMaxScalerModel): MinMaxScalerServingModel = new MinMaxScalerServingModel(stage)
}