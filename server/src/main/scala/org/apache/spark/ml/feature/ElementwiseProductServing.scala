package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.sql.types.DataType

class ElementwiseProductServing(stage: ElementwiseProduct)
  extends UnaryTransformerServing[Vector, Vector, ElementwiseProductServing] {
  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  override protected def createTransformFunc: Vector => Vector = {
    require(params.contains(stage.scalingVec), s"transformation requires a weight vector")
    val elemScaler = new feature.ElementwiseProduct($(stage.scalingVec))
    v => elemScaler.transform(v)
  }

  override val uid: String = stage.uid

  override def copy(extra: ParamMap): ElementwiseProductServing = {
    new ElementwiseProductServing(stage.copy(extra))
  }

  /**
    * Returns the data type of the output column.
    */
  override protected def outputDataType: DataType = new VectorUDT()
}

object ElementwiseProductServing {
  def apply(stage: ElementwiseProduct): ElementwiseProductServing = new ElementwiseProductServing(stage)
}