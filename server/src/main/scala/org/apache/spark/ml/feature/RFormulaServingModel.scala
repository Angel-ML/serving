package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, UDF}
import org.apache.spark.ml.feature.RFormulaModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.utils.ModelUtils
import org.apache.spark.sql.types._

class RFormulaServingModel(stage: RFormulaModel) extends ServingModel[RFormulaServingModel] {

  override def copy(extra: ParamMap): RFormulaServingModel = {
    new RFormulaServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    checkCanTransform(dataset.schema)
    transformLabel(ModelUtils.transModel(stage.pipelineModel).transform(dataset))
  }

  override def transformSchema(schema: StructType): StructType = {
    checkCanTransform(schema)
    val withFeatures = stage.pipelineModel.transformSchema(schema)
    if (stage.resolvedFormula.label.isEmpty || hasLabelCol(withFeatures)) {
      withFeatures
    } else if (schema.exists(_.name == stage.resolvedFormula.label)) {
      val nullable = schema(stage.resolvedFormula.label).dataType match {
        case _: NumericType | BooleanType => false
        case _ => true
      }
      StructType(withFeatures.fields :+ StructField($(stage.labelCol), DoubleType, nullable))
    } else {
      // Ignore the label field. This is a hack so that this transformer can also work on test
      // datasets in a Pipeline.
      withFeatures
    }
  }

  override val uid: String = stage.uid

  private def transformLabel(dataset: SDFrame): SDFrame = {
    val labelName = stage.resolvedFormula.label//todo: stage.resolvedFormula whether is accessible
    if (labelName.isEmpty || hasLabelCol(dataset.schema)) {
      dataset
    } else if (dataset.schema.exists(_.name == labelName)) {
      dataset.schema(labelName).dataType match {
        case _: NumericType | BooleanType =>
          //todo: dataset(labelName).cast(DoubleType)
          val labelUDF = UDF.make[Double, String](labelName => dataset(labelName).asInstanceOf[Double])
          dataset.withColum(labelUDF.apply($(stage.labelCol), dataset(labelName)))
        case other =>
          throw new IllegalArgumentException("Unsupported type for label: " + other)
      }
    } else {
      // Ignore the label field. This is a hack so that this transformer can also work on test
      // datasets in a Pipeline.
      dataset
    }
  }

  private def checkCanTransform(schema: StructType) {
    val columnNames = schema.map(_.name)
    require(!columnNames.contains($(stage.featuresCol)), "Features column already exists.")
    require(
      !columnNames.contains($(stage.labelCol)) || schema($(stage.labelCol)).dataType.isInstanceOf[NumericType],
      "Label column already exists and is not of type NumericType.")
  }

  protected def hasLabelCol(schema: StructType): Boolean = {
    schema.map(_.name).contains($(stage.labelCol))
  }
}

object RFormulaServingModel {
  def apply(stage: RFormulaModel): RFormulaServingModel = new RFormulaServingModel(stage)
}