package org.apache.spark.ml.tunning

import org.apache.spark.ml.data.SDFrame
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.types.StructType

class TrainValidationSplitServingModel(stage: TrainValidationSplitModel)
  extends ServingModel[TrainValidationSplitServingModel] {

  override def copy(extra: ParamMap): TrainValidationSplitServingModel = {
    new TrainValidationSplitServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, logging = true)
    ModelUtils.transModel(stage.bestModel).transform(dataset)
  }

  override def transformSchema(schema: StructType): StructType = {
    stage.bestModel.transformSchema(schema)
  }

  override val uid: String = stage.uid
}

object TrainValidationSplitServingModel {
  def apply(stage: TrainValidationSplitModel): TrainValidationSplitServingModel =
    new TrainValidationSplitServingModel(stage)
}