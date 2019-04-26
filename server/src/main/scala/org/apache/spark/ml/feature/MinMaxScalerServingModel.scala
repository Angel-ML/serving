package org.apache.spark.ml.feature

import java.util

import org.apache.spark.ml.data.{SCol, SDFrame, SRow, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types._

class MinMaxScalerServingModel(stage: MinMaxScalerModel)
  extends ServingModel[MinMaxScalerServingModel] {

  override def copy(extra: ParamMap): MinMaxScalerServingModel = {
    new MinMaxScalerServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    val originalRange = (stage.originalMax.asBreeze - stage.originalMin.asBreeze).toArray
    val minArray = stage.originalMin.toArray
    val reScaleUDF = UDF.make[Vector, Vector](feature => {
      val scale = stage.getMax - stage.getMin
      val values = feature.toArray
      val size = feature.size
      var i = 0
      while (i < size) {
        if (!values(i).isNaN) {
          val raw = if (originalRange(i) != 0) (values(i) - minArray(i)) / originalRange(i) else 0.5
          values(i) = raw * scale + stage.getMin
        }
        i += 1
      }
      Vectors.dense(values)
    }, false)
    dataset.withColum(reScaleUDF.apply(stage.getOutputCol, SCol(stage.getInputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemaImpl(schema)
  }

  def validateAndTransformSchemaImpl(schema: StructType): StructType = {
    require(stage.getMin < stage.getMax, s"The specified min(${stage.getMin}) is larger or equal to max(${stage.getMax})")
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
      throw new Exception (s"inputCol or inputCols of ${stage} is not defined!")
    }
  }

  override def prepareData(feature: util.Map[String, _]): SDFrame = {
    if (stage.isDefined(stage.inputCol)) {
      val featureName = feature.keySet.toArray
      if (!featureName.contains(stage.getInputCol)) {
        throw new Exception (s"the ${stage.getInputCol} is not included in the input col(s)")
      } else if (!feature.get(stage.getInputCol).isInstanceOf[Vector]) {
        throw new Exception (s"the type of col ${stage.getInputCol} is not Vector")
      } else {
        val schema = new StructType().add(new StructField(stage.getInputCol, new VectorUDT, true))
        val rows =  Array(new SRow(Array(feature.get(stage.getInputCol))))
        new SDFrame(rows)(schema)
      }
    } else {
      throw new Exception (s"inputCol or inputCols of ${stage} is not defined!")
    }
  }
}

object MinMaxScalerServingModel {
  def apply(stage: MinMaxScalerModel): MinMaxScalerServingModel = new MinMaxScalerServingModel(stage)
}