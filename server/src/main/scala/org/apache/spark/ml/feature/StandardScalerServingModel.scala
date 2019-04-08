package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.util.SchemaUtils

class StandardScalerServingModel(stage: StandardScalerModel)
  extends ServingModel[StandardScalerServingModel] {

  override def copy(extra: ParamMap): StandardScalerServingModel = {
    new StandardScalerServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    val scaleUDF = UDF.make[Vector, Vector](trans, false)
    dataset.withColum(scaleUDF.apply(stage.getOutputCol, SCol(stage.getInputCol)))
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

  private lazy val shift: Array[Double] = stage.mean.toArray

  def trans(features:Vector): Vector ={
    require(stage.mean.size == features.size)
    if (stage.getWithMean) {
      // By default, Scala generates Java methods for member variables. So every time when
      // the member variables are accessed, `invokespecial` will be called which is expensive.
      // This can be avoid by having a local reference of `shift`.
      val localShift = shift
      // Must have a copy of the values since it will be modified in place
      val values = features match {
        // specially handle DenseVector because its toArray does not clone already
        case d: DenseVector => d.values.clone()
        case v: Vector => v.toArray
      }
      val size = values.length
      if (stage.getWithStd) {
        var i = 0
        while (i < size) {
          values(i) = if (stage.std(i) != 0.0) (values(i) - localShift(i)) * (1.0 / stage.std(i)) else 0.0
          i += 1
        }
      } else {
        var i = 0
        while (i < size) {
          values(i) -= localShift(i)
          i += 1
        }
      }
      Vectors.dense(values)
    } else if (stage.getWithStd) {
      features match {
        case DenseVector(vs) =>
          val values = vs.clone()
          val size = values.length
          var i = 0
          while(i < size) {
            values(i) *= (if (stage.std(i) != 0.0) 1.0 / stage.std(i) else 0.0)
            i += 1
          }
          Vectors.dense(values)
        case SparseVector(size, indices, vs) =>
          // For sparse vector, the `index` array inside sparse vector object will not be changed,
          // so we can re-use it to save memory.
          val values = vs.clone()
          val nnz = values.length
          var i = 0
          while (i < nnz) {
            values(i) *= (if (stage.std(indices(i)) != 0.0) 1.0 / stage.std(indices(i)) else 0.0)
            i += 1
          }
          Vectors.sparse(size, indices, values)
        case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
      }
    } else {
      // Note that it's safe since we always assume that the data in RDD should be immutable.
      features
    }
  }
}

object StandardScalerServingModel {
  def apply(stage: StandardScalerModel): StandardScalerServingModel = new StandardScalerServingModel(stage)
}
