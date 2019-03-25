package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.feature
import org.apache.spark.sql.types.DataType

class NormalizerServing(stage: Normalizer) extends UnaryTransformerServing[Vector, Vector, NormalizerServing] {
  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  override protected def createTransformFunc: Vector => Vector = {
    val normalizer = new feature.Normalizer($(stage.p))
    vector => normalizer.transform(OldVectors.fromML(vector)).asML
  }

  override val uid: String = stage.uid

  override def copy(extra: ParamMap): NormalizerServing = {
    new NormalizerServing(stage.copy(extra))
  }

  /**
    * Returns the data type of the output column.
    */
  override protected def outputDataType: DataType = new VectorUDT()
}

object NormalizerServing {
  def apply(stage: Normalizer): NormalizerServing = new NormalizerServing(stage)
}