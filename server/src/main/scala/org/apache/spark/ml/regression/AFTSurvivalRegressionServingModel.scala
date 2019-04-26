package org.apache.spark.ml.regression

import java.util

import org.apache.spark.ml.data.{SCol, SDFrame, SRow, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

class AFTSurvivalRegressionServingModel(stage: AFTSurvivalRegressionModel)
  extends ServingModel[AFTSurvivalRegressionServingModel] {

  override def copy(extra: ParamMap): AFTSurvivalRegressionServingModel = {
    new AFTSurvivalRegressionServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    val predictUDF = {
      UDF.make[Double, Vector](stage.predict, false)
    }
    val predictQuantilesUDF = {
      UDF.make[Vector, Vector](stage.predictQuantiles, false)
    }
    var output = dataset
    if (stage.hasQuantilesCol) {
      output = dataset.withColum(predictUDF.apply(stage.getPredictionCol, SCol(stage.getFeaturesCol)))
        .withColum(predictQuantilesUDF.apply(stage.getQuantilesCol, SCol(stage.getFeaturesCol)))
    } else {
      output = dataset.withColum(predictUDF.apply(stage.getPredictionCol, SCol(stage.getFeaturesCol)))
    }
    output
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemaImpl(schema, false)
  }

  /**
    * Validates and transforms the input schema with the provided param map.
    * @param schema input schema
    * @param fitting whether this is in fitting or prediction
    * @return output schema
    */
  def validateAndTransformSchemaImpl(
                                      schema: StructType,
                                      fitting: Boolean): StructType = {
    SchemaUtils.checkColumnType(schema, stage.getFeaturesCol, new VectorUDT)
    if (fitting) {
      SchemaUtils.checkNumericType(schema, stage.getCensorCol)
      SchemaUtils.checkNumericType(schema, stage.getLabelCol)
    }

    val schemaWithQuantilesCol = if (stage.hasQuantilesCol) {
      SchemaUtils.appendColumn(schema, stage.getQuantilesCol, new VectorUDT)
    } else schema

    SchemaUtils.appendColumn(schemaWithQuantilesCol, stage.getPredictionCol, DoubleType)
  }

  override val uid: String = stage.uid

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.featuresCol)) {
      val schema = new StructType().add(new StructField(stage.getFeaturesCol, new VectorUDT, true))
      new SDFrame(rows)(schema)
    } else {
      throw new Exception (s"featuresCol of ${stage} is not defined!")
    }
  }

  override def prepareData(feature: util.Map[String, _]): SDFrame = {
    if (stage.isDefined(stage.featuresCol)) {
      val featureName = feature.keySet.toArray
      if (!featureName.contains(stage.getFeaturesCol)) {
        throw new Exception (s"the ${stage.getFeaturesCol} is not included in the input col(s)")
      } else if (!feature.get(stage.getFeaturesCol).isInstanceOf[Vector]) {
        throw new Exception (s"the type of col ${stage.getFeaturesCol} is not Vector")
      } else {
        val schema = new StructType().add(new StructField(stage.getFeaturesCol, new VectorUDT, true))
        val rows =  Array(new SRow(Array(feature.get(stage.getFeaturesCol))))
        new SDFrame(rows)(schema)
      }
    } else {
      throw new Exception (s"featuresCol of ${stage} is not defined!")
    }
  }
}

object AFTSurvivalRegressionServingModel{

  def apply(stage: AFTSurvivalRegressionModel): AFTSurvivalRegressionServingModel =
    new AFTSurvivalRegressionServingModel(stage)
}
