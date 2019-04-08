package org.apache.spark.ml.feature

import org.apache.commons.math3.util.CombinatoricsUtils
import org.apache.spark.ml.data.{SDFrame, UDF}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.DataType

import scala.collection.mutable

class PolynomialExpansionServing(stage: PolynomialExpansion)
  extends UnaryTransformerServing[Vector, Vector, PolynomialExpansionServing, PolynomialExpansion](stage) {
  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  protected def createTransformFunc: Vector => Vector = { v =>
    PolynomialExpansionServing.expand(v,stage.getDegree)
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    val transformUDF = UDF.make[Vector, Vector](createTransformFunc, false)
    dataset.withColum(transformUDF.apply(stage.getOutputCol, dataset(stage.getInputCol)))
  }

  override val uid: String = stage.uid

  override def copy(extra: ParamMap): PolynomialExpansionServing = {
    new PolynomialExpansionServing(stage.copy(extra))
  }

  /**
    * Returns the data type of the output column.
    */
  override def outputDataType: DataType = new VectorUDT()
}

object PolynomialExpansionServing {
  def apply(stage: PolynomialExpansion): PolynomialExpansionServing = new PolynomialExpansionServing(stage)

  private def getPolySize(numFeatures: Int, degree: Int): Int = {
    val n = CombinatoricsUtils.binomialCoefficient(numFeatures + degree, degree)
    require(n <= Integer.MAX_VALUE)
    n.toInt
  }

  private def expandDense(
                           values: Array[Double],
                           lastIdx: Int,
                           degree: Int,
                           multiplier: Double,
                           polyValues: Array[Double],
                           curPolyIdx: Int): Int = {
    if (multiplier == 0.0) {
      // do nothing
    } else if (degree == 0 || lastIdx < 0) {
      if (curPolyIdx >= 0) { // skip the very first 1
        polyValues(curPolyIdx) = multiplier
      }
    } else {
      val v = values(lastIdx)
      val lastIdx1 = lastIdx - 1
      var alpha = multiplier
      var i = 0
      var curStart = curPolyIdx
      while (i <= degree && alpha != 0.0) {
        curStart = expandDense(values, lastIdx1, degree - i, alpha, polyValues, curStart)
        i += 1
        alpha *= v
      }
    }
    curPolyIdx + getPolySize(lastIdx + 1, degree)
  }

  private def expandSparse(
                            indices: Array[Int],
                            values: Array[Double],
                            lastIdx: Int,
                            lastFeatureIdx: Int,
                            degree: Int,
                            multiplier: Double,
                            polyIndices: mutable.ArrayBuilder[Int],
                            polyValues: mutable.ArrayBuilder[Double],
                            curPolyIdx: Int): Int = {
    if (multiplier == 0.0) {
      // do nothing
    } else if (degree == 0 || lastIdx < 0) {
      if (curPolyIdx >= 0) { // skip the very first 1
        polyIndices += curPolyIdx
        polyValues += multiplier
      }
    } else {
      // Skip all zeros at the tail.
      val v = values(lastIdx)
      val lastIdx1 = lastIdx - 1
      val lastFeatureIdx1 = indices(lastIdx) - 1
      var alpha = multiplier
      var curStart = curPolyIdx
      var i = 0
      while (i <= degree && alpha != 0.0) {
        curStart = expandSparse(indices, values, lastIdx1, lastFeatureIdx1, degree - i, alpha,
          polyIndices, polyValues, curStart)
        i += 1
        alpha *= v
      }
    }
    curPolyIdx + getPolySize(lastFeatureIdx + 1, degree)
  }

  private def expand(dv: DenseVector, degree: Int): DenseVector = {
    val n = dv.size
    val polySize = getPolySize(n, degree)
    val polyValues = new Array[Double](polySize - 1)
    expandDense(dv.values, n - 1, degree, 1.0, polyValues, -1)
    new DenseVector(polyValues)
  }

  private def expand(sv: SparseVector, degree: Int): SparseVector = {
    val polySize = getPolySize(sv.size, degree)
    val nnz = sv.values.length
    val nnzPolySize = getPolySize(nnz, degree)
    val polyIndices = mutable.ArrayBuilder.make[Int]
    polyIndices.sizeHint(nnzPolySize - 1)
    val polyValues = mutable.ArrayBuilder.make[Double]
    polyValues.sizeHint(nnzPolySize - 1)
    expandSparse(
      sv.indices, sv.values, nnz - 1, sv.size - 1, degree, 1.0, polyIndices, polyValues, -1)
    new SparseVector(polySize - 1, polyIndices.result(), polyValues.result())
  }

  private def expand(v: Vector, degree: Int): Vector = {
    v match {
      case dv: DenseVector => expand(dv, degree)
      case sv: SparseVector => expand(sv, degree)
      case _ => throw new IllegalArgumentException
    }
  }
}
