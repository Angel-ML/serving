package org.apache.spark.ml.feature
import org.apache.spark.ml.data.{SDFrame, SRow, UDF}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._

class RegexTokenizerServing(stage: RegexTokenizer)
  extends UnaryTransformerServing[String, Seq[String], RegexTokenizerServing, RegexTokenizer](stage) {
  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  def createTransformFunc: String => Seq[String] = { originStr =>
    val re = stage.getPattern.r
    val str = if (stage.getToLowercase) originStr.toLowerCase() else originStr
    val tokens = if (stage.getGaps) re.split(str).toSeq else re.findAllIn(str).toSeq
    val minLength = stage.getMinTokenLength
    tokens.filter(_.length >= minLength)
  }

  override val uid: String = stage.uid

  override def copy(extra: ParamMap): RegexTokenizerServing = {
    new RegexTokenizerServing(stage.copy(extra))
  }

  /**
    * Returns the data type of the output column.
    */
  def outputDataType: DataType = new ArrayType(StringType, true)

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    val transformUDF = UDF.make[Seq[String], String](createTransformFunc, false)
    dataset.withColum(transformUDF.apply(stage.getOutputCol, dataset(stage.getInputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema(stage.getInputCol).dataType
    validateInputType(inputType)
    if (schema.fieldNames.contains(stage.getOutputCol)) {
      throw new IllegalArgumentException(s"Output column ${stage.getOutputCol} already exists.")
    }
    val outputFields = schema.fields :+
      StructField(stage.getOutputCol, outputDataType, nullable = false)
    StructType(outputFields)
  }

  /**
    * Validates the input type. Throw an exception if it is invalid.
    */
  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.inputCol)) {
      val schema = new StructType().add(new StructField(stage.getInputCol, StringType, true))
      new SDFrame(rows)(schema)
    } else {
      throw new Exception (s"inputCol or inputCols of ${stage} is not defined!")
    }
  }
}

object RegexTokenizerServing {
  def apply(stage: RegexTokenizer): RegexTokenizerServing = new RegexTokenizerServing(stage)
}