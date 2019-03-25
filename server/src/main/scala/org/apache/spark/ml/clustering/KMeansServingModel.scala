package org.apache.spark.ml.clustering

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.StructType

class KMeansServingModel(stage: KMeansModel) extends ServingModel[KMeansServingModel] with KMeansParams {

  override def copy(extra: ParamMap): KMeansServingModel = {
    new KMeansServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)

    val predictUDF = UDF.make[Int, Vector](features => stage.predict(features))
    dataset.withColum(predictUDF.apply(${stage.predictionCol}, SCol(${stage.featuresCol})))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override val uid: String = stage.uid
}

object KMeansServingModel {
  def apply(stage: KMeansModel): KMeansServingModel = new KMeansServingModel(stage)
}