package org.apache.spark.ml.feature
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

class RegexTokenizerServing(stage: RegexTokenizer)
  extends UnaryTransformerServing[String, Seq[String], RegexTokenizerServing] {
  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  override protected def createTransformFunc: String => Seq[String] = { originStr =>
    val re = $(stage.pattern).r
    val str = if ($(stage.toLowercase)) originStr.toLowerCase() else originStr
    val tokens = if ($(stage.gaps)) re.split(str).toSeq else re.findAllIn(str).toSeq
    val minLength = $(stage.minTokenLength)
    tokens.filter(_.length >= minLength)
  }

  override val uid: String = stage.uid

  override def copy(extra: ParamMap): RegexTokenizerServing = {
    new RegexTokenizerServing(stage.copy(extra))
  }

  /**
    * Returns the data type of the output column.
    */
  override protected def outputDataType: DataType = new ArrayType(StringType, true)
}

object RegexTokenizerServing {
  def apply(stage: RegexTokenizer): RegexTokenizerServing = new RegexTokenizerServing(stage)
}