package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, UDF}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.sql.types.DataType

class ElementwiseProductServing(stage: ElementwiseProduct)
  extends UnaryTransformerServing[Vector, Vector, ElementwiseProductServing, ElementwiseProduct](stage) {
  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  protected def createTransformFunc: Vector => Vector = {
    require(stage.params.contains(stage.scalingVec), s"transformation requires a weight vector")
    val elemScaler = new feature.ElementwiseProduct(stage.getScalingVec)
    v => elemScaler.transform(v)
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    val transformUDF = UDF.make[Vector, Vector](feature => this.createTransformFunc(feature))
    dataset.withColum(transformUDF.apply(stage.getOutputCol, dataset(stage.getInputCol)))
  }

  override val uid: String = stage.uid

  override def copy(extra: ParamMap): ElementwiseProductServing = {
    new ElementwiseProductServing(stage.copy(extra))
  }

  /**
    * Returns the data type of the output column.
    */
  override def outputDataType: DataType = new VectorUDT()
}

object ElementwiseProductServing {
  def apply(stage: ElementwiseProduct): ElementwiseProductServing = new ElementwiseProductServing(stage)
}