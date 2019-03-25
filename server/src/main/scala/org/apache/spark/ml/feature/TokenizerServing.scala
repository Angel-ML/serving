package org.apache.spark.ml.feature

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

class TokenizerServing(stage: Tokenizer) extends UnaryTransformerServing[String, Seq[String], TokenizerServing] {

  override val uid: String = stage.uid

  override protected def createTransformFunc: String => Seq[String] = {
    _.toLowerCase.split("\\s")
  }

  override def copy(extra: ParamMap): TokenizerServing = {
    new TokenizerServing(stage.copy(extra))
  }

  /**
    * Returns the data type of the output column.
    */
  override protected def outputDataType: DataType = new ArrayType(StringType, true)
}

object TokenizerServing {
  def apply(stage: Tokenizer): TokenizerServing = new TokenizerServing(stage)
}