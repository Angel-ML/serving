package org.apache.spark.ml.transformer

import org.apache.spark.ml.data.SDFrame
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType

class ServingPipelineModel(override val uid: String,
                           val stages: Array[ServingTrans]) extends ServingModel[ServingPipelineModel] {
  override def copy(extra: ParamMap): ServingPipelineModel = ???

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    stages.foldLeft(dataset)((cur, transformer) => transformer.transform(cur))
  }

  override def transformSchema(schema: StructType): StructType = ???
}