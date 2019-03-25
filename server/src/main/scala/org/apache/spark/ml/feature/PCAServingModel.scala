package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.mllib.feature
import org.apache.spark.sql.types.StructType
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix, DenseVector => OldDenseVector,
  Matrices => OldMatrices, Vectors => OldVectors}

class PCAServingModel(stage: PCAModel) extends ServingModel[PCAServingModel] with PCAParams {

  override def copy(extra: ParamMap): PCAServingModel = {
    new PCAServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    val pcaModel = new feature.PCAModel($(stage.k),
      OldMatrices.fromML(stage.pc).asInstanceOf[OldDenseMatrix],
      OldVectors.fromML(stage.explainedVariance).asInstanceOf[OldDenseVector])
    val pcaUDF = UDF.make[Vector, Vector](
      features => pcaModel.transform(OldVectors.fromML(features)).asML)
    dataset.withColum(pcaUDF.apply($(stage.outputCol), SCol($(stage.inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override val uid: String = stage.uid
}

object PCAServingModel {
  def apply(stage: PCAModel): PCAServingModel = new PCAServingModel(stage)
}
