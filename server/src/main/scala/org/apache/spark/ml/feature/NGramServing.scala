package org.apache.spark.ml.feature
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

class NGramServing(stage: NGram) extends UnaryTransformerServing[Seq[String], Seq[String], NGramServing] {
  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  override protected def createTransformFunc: Seq[String] => Seq[String] = {
    _.iterator.sliding($(stage.n)).withPartial(false).map(_.mkString(" ")).toSeq
  }

  override val uid: String = stage.uid

  override def copy(extra: ParamMap): NGramServing = {
    new NGramServing(stage.copy(extra))
  }

  /**
    * Returns the data type of the output column.
    */
  override protected def outputDataType: DataType = new ArrayType(StringType, false)
}

object NGramServing {
  def apply(stage: NGram): NGramServing = new NGramServing(stage)
}