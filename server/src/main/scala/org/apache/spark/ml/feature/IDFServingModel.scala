package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.util.SchemaUtils

class IDFServingModel(stage: IDFModel)  extends ServingModel[IDFServingModel] {

  override def copy(extra: ParamMap): IDFServingModel = {
    new IDFServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    val idfUDF = UDF.make[Vector, Vector](features => {
      trans(stage.idf, features)
    })
    dataset.withColum(idfUDF.apply(stage.getOutputCol, SCol(stage.getInputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemaImpl(schema)
  }

  /**
    * Validate and transform the input schema.
    */
  def validateAndTransformSchemaImpl(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, stage.getInputCol, new VectorUDT)
    SchemaUtils.appendColumn(schema, stage.getOutputCol, new VectorUDT)
  }

  override val uid: String = stage.uid

  def trans(idf: Vector, features: Vector): Vector ={
    val n = features.size
    features match {
      case SparseVector(size, indices, values) =>
        val nnz = indices.length
        val newValues = new Array[Double](nnz)
        var k = 0
        while (k < nnz) {
          newValues(k) = values(k) * idf(indices(k))
          k += 1
        }
        Vectors.sparse(n, indices, newValues)
      case DenseVector(values) =>
        val newValues = new Array[Double](n)
        var j = 0
        while (j < n) {
          newValues(j) = values(j) * idf(j)
          j += 1
        }
        Vectors.dense(newValues)
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }
}

object IDFServingModel {
  def apply(stage: IDFModel): IDFServingModel = new IDFServingModel(stage)
}