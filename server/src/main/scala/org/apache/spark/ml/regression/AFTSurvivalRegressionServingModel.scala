package org.apache.spark.ml.regression

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.{Vector}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.StructType

class AFTSurvivalRegressionServingModel(stage: AFTSurvivalRegressionModel)
  extends ServingModel[AFTSurvivalRegressionServingModel] with AFTSurvivalRegressionParams{

  override def copy(extra: ParamMap): AFTSurvivalRegressionServingModel = {
    new AFTSurvivalRegressionServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    val predictUDF = {
      UDF.make[Double, Vector](feature =>
        stage.predict(feature))
    }
    val predictQuantilesUDF = {
      UDF.make[Vector, Vector](feature =>
        stage.predictQuantiles(feature))
    }
    if (hasQuantilesCol) {
      dataset.withColum(predictUDF.apply(${stage.predictionCol}, SCol(${stage.featuresCol})))
        .withColum(predictQuantilesUDF.apply(${stage.quantilesCol}, SCol(${stage.featuresCol})))
    } else {
      dataset.withColum(predictUDF.apply(${stage.predictionCol}, SCol(${stage.featuresCol})))
    }

  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, false)
  }

  override val uid: String = stage.uid
}

object AFTSurvivalRegressionServingModel{

  def apply(stage: AFTSurvivalRegressionModel): AFTSurvivalRegressionServingModel =
    new AFTSurvivalRegressionServingModel(stage)
}
