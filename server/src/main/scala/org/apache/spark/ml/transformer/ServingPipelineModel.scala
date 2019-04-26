package org.apache.spark.ml.transformer

import java.util

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType

class ServingPipelineModel(override val uid: String,
                           val stages: Array[ServingTrans]) extends ServingModel[ServingPipelineModel] {

  override def copy(extra: ParamMap): ServingPipelineModel = {
//    val stagesCopy: Array[ServingTrans] = stages.foreach(transformer => transformer.copy(extra))
//    new ServingPipelineModel(uid, stagesCopy)
    ???
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    stages.foldLeft(dataset)((cur, transformer) => transformer.transform(cur))
  }

  override def transformSchema(schema: StructType): StructType = {
    stages.foldLeft(schema)((cur, transformer) => transformer.transformSchema(cur))
  }

  override def prepareData(rows: Array[SRow]): SDFrame = {
    stages(0).prepareData(rows)
  }

  override def prepareData(feature: util.Map[String, _]): SDFrame = {
    stages(0).prepareData(feature)
  }
}