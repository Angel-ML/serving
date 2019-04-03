package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.feature
import org.apache.spark.sql.types.DataType

class NormalizerServing(stage: Normalizer)
  extends UnaryTransformerServing[Vector, Vector, NormalizerServing, Normalizer](stage) {
  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  protected def createTransformFunc: Vector => Vector = {
    val normalizer = new feature.Normalizer(stage.getP)
    vector => normalizer.transform(OldVectors.fromML(vector)).asML
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    val transformUDF = UDF.make[Vector, Vector](feature => this.createTransformFunc(feature))
    dataset.withColum(transformUDF.apply(stage.getOutputCol, dataset(stage.getInputCol)))
  }

  override val uid: String = stage.uid

  override def copy(extra: ParamMap): NormalizerServing = {
    new NormalizerServing(stage.copy(extra))
  }

  /**
    * Returns the data type of the output column.
    */
  override def outputDataType: DataType = new VectorUDT()
}

object NormalizerServing {
  def apply(stage: Normalizer): NormalizerServing = new NormalizerServing(stage)
}