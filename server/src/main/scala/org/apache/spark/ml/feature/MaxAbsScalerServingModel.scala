package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.StructType

class MaxAbsScalerServingModel(stage: MaxAbsScalerModel)
  extends ServingModel[MaxAbsScalerServingModel] with MaxAbsScalerParams {

  override def copy(extra: ParamMap): MaxAbsScalerServingModel = {
    new MaxAbsScalerServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    val maxAbsUnzero = Vectors.dense(stage.maxAbs.toArray.map(x => if (x == 0) 1 else x))
    val reScaleUDF = UDF.make[Vector, Vector](feature => {
      val brz = feature.asBreeze / maxAbsUnzero.asBreeze
      Vectors.fromBreeze(brz)
    })
    dataset.withColum(reScaleUDF.apply($(stage.outputCol), SCol($(stage.inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override val uid: String = stage.uid
}

object MaxAbsScalerServingModel {
  def apply(stage: MaxAbsScalerModel): MaxAbsScalerServingModel = new MaxAbsScalerServingModel(stage)
}