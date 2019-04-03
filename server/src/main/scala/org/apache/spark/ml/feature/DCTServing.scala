package org.apache.spark.ml.feature

import edu.emory.mathcs.jtransforms.dct.DoubleDCT_1D
import org.apache.spark.ml.data.{SDFrame, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.DataType

class DCTServing(stage: DCT) extends UnaryTransformerServing[Vector, Vector, DCTServing, DCT](stage) {
  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  protected def createTransformFunc: Vector => Vector = { vec =>
    val result = vec.toArray
    val jTransformer = new DoubleDCT_1D(result.length)
    if (stage.getInverse) jTransformer.inverse(result, true) else jTransformer.forward(result, true)
    Vectors.dense(result)
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    val transformUDF = UDF.make[Vector, Vector](feature => this.createTransformFunc(feature))
    dataset.withColum(transformUDF.apply(stage.getOutputCol, dataset(stage.getInputCol)))
  }

  override val uid: String = stage.uid

  override def copy(extra: ParamMap): DCTServing = {
    new DCTServing(stage.copy(extra))
  }

  /**
    * Returns the data type of the output column.
    */
  override def outputDataType: DataType = new VectorUDT

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.isInstanceOf[VectorUDT], s"Input type must be VectorUDT but got $inputType.")
  }
}

object DCTServing {
  def apply(stage: DCT): DCTServing = new DCTServing(stage)
}