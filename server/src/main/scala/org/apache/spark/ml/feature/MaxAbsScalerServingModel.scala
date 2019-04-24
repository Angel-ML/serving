package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SCol, SDFrame, SRow, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types._

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
    }, false)
    dataset.withColum(reScaleUDF.apply(stage.getOutputCol, SCol(stage.getInputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemaImpl(schema)
  }

  /** Validates and transforms the input schema. */
  def validateAndTransformSchemaImpl(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, stage.getInputCol, new VectorUDT)
    require(!schema.fieldNames.contains(stage.getOutputCol),
      s"Output column ${stage.getOutputCol} already exists.")
    val outputFields = schema.fields :+ StructField(stage.getOutputCol, new VectorUDT, false)
    StructType(outputFields)
  }

  override val uid: String = stage.uid

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.inputCol)) {
      val schema = new StructType().add(new StructField(stage.getInputCol, new VectorUDT, true))
      new SDFrame(rows)(schema)
    } else {
      throw new Exception (s"featuresCol of ${stage} is not defined!")
    }
  }
}

object MaxAbsScalerServingModel {
  def apply(stage: MaxAbsScalerModel): MaxAbsScalerServingModel = new MaxAbsScalerServingModel(stage)
}