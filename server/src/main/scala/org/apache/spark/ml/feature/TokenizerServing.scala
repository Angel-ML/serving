package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, SRow, UDF}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._

class TokenizerServing(stage: Tokenizer) extends UnaryTransformerServing[String, Seq[String], TokenizerServing, Tokenizer](stage) {

  override val uid: String = stage.uid

  def createTransformFunc: String => Seq[String] = {
    _.toLowerCase.split("\\s")
  }

  override def copy(extra: ParamMap): TokenizerServing = {
    new TokenizerServing(stage.copy(extra))
  }

  /**
    * Returns the data type of the output column.
    */
  def outputDataType: DataType = new ArrayType(StringType, true)

  override def transform(dataset: SDFrame): SDFrame = {
    val stuctType = transformSchema(dataset.schema, true)
    val metadata = stuctType(stage.getOutputCol).metadata
    val transformUDF = UDF.make[Seq[String], String](createTransformFunc, false)
    dataset.withColum(transformUDF.apply(stage.getOutputCol, dataset(stage.getInputCol))
      .setSchema(stage.getOutputCol, metadata))
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

object TokenizerServing {
  def apply(stage: Tokenizer): TokenizerServing = new TokenizerServing(stage)
}