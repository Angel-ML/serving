package org.apache.spark.ml.clustering

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.StructType

class BisectingKMeansServingModel(stage: BisectingKMeansModel)
  extends ServingModel[BisectingKMeansServingModel] with BisectingKMeansParams {

  override def copy(extra: ParamMap): BisectingKMeansServingModel = {
    new BisectingKMeansServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    //todo:predict whether is accessible
    val predictUDF = UDF.make[Int, Vector](features => stage.predict(features))
    dataset.withColum(predictUDF(${stage.predictionCol}, SCol(${stage.featuresCol})))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override val uid: String = stage.uid
}

object BisectingKMeansServingModel {
  def apply(stage: BisectingKMeansModel): BisectingKMeansServingModel =
    new BisectingKMeansServingModel(stage)
}
