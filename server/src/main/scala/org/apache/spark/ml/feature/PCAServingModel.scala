package org.apache.spark.ml.feature

import java.util

import org.apache.spark.ml.data.{SCol, SDFrame, SRow, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.feature
import org.apache.spark.sql.types._
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix, DenseVector => OldDenseVector, Matrices => OldMatrices, Vectors => OldVectors}

class PCAServingModel(stage: PCAModel) extends ServingModel[PCAServingModel] {

  override def copy(extra: ParamMap): PCAServingModel = {
    new PCAServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    val pcaModel = new feature.PCAModel(stage.getK,
      OldMatrices.fromML(stage.pc).asInstanceOf[OldDenseMatrix],
      OldVectors.fromML(stage.explainedVariance).asInstanceOf[OldDenseVector])
    val pcaUDF = UDF.make[Vector, Vector](
      features => pcaModel.transform(OldVectors.fromML(features)).asML, false)
    dataset.withColum(pcaUDF.apply(stage.getOutputCol, SCol(stage.getInputCol)))
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

object PCAServingModel {
  def apply(stage: PCAModel): PCAServingModel = new PCAServingModel(stage)
}
