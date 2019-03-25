package org.apache.spark.ml.feature

import edu.emory.mathcs.jtransforms.dct.DoubleDCT_1D
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.DataType

class DCTServing(stage: DCT) extends UnaryTransformerServing[Vector, Vector, DCTServing] {
  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  override protected def createTransformFunc: Vector => Vector = { vec =>
    val result = vec.toArray
    val jTransformer = new DoubleDCT_1D(result.length)
    if ($(stage.inverse)) jTransformer.inverse(result, true) else jTransformer.forward(result, true)
    Vectors.dense(result)
  }

  override val uid: String = stage.uid

  override def copy(extra: ParamMap): DCTServing = {
    new DCTServing(stage.copy(extra))
  }

  /**
    * Returns the data type of the output column.
    */
  override protected def outputDataType: DataType = new VectorUDT
}

object DCTServing {
  def apply(stage: DCT): DCTServing = new DCTServing(stage)
}