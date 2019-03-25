package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.StructType

class MinMaxScalerServingModel(stage: MinMaxScalerModel)
  extends ServingModel[MinMaxScalerServingModel] with MinMaxScalerParams {

  override def copy(extra: ParamMap): MinMaxScalerServingModel = {
    new MinMaxScalerServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    val originalRange = (stage.originalMax.asBreeze - stage.originalMin.asBreeze).toArray
    val minArray = stage.originalMin.toArray
    val reScaleUDF = UDF.make[Vector, Vector](feature => {
      val scale = $(stage.max) - $(stage.min)
      val values = feature.toArray
      val size = feature.size
      var i = 0
      while (i < size) {
        if (!values(i).isNaN) {
          val raw = if (originalRange(i) != 0) (values(i) - minArray(i)) / originalRange(i) else 0.5
          values(i) = raw * scale + $(stage.min)
        }
        i += 1
      }
      Vectors.dense(values)
    })
    dataset.withColum(reScaleUDF.apply($(stage.outputCol), SCol($(stage.inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override val uid: String = stage.uid
}

object MinMaxScalerServingModel {
  def apply(stage: MinMaxScalerModel): MinMaxScalerServingModel = new MinMaxScalerServingModel(stage)
}