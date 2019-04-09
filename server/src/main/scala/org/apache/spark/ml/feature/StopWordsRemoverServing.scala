package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

class StopWordsRemoverServing(stage: StopWordsRemover) extends ServingTrans{
  override def transform(dataset: SDFrame): SDFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val tUDF = if (stage.getCaseSensitive) {
      val stopWordsSet = stage.getStopWords.toSet
      UDF.make[Seq[String], Seq[String]](terms => terms.filter(s => !stopWordsSet.contains(s)), false)
    } else {
      // TODO: support user locale (SPARK-15064)
      val toLower = (s: String) => if (s != null) s.toLowerCase else s
      val lowerStopWords = stage.getStopWords.map(toLower(_)).toSet
      UDF.make[Seq[String], Seq[String]](terms => terms.filter(s => !lowerStopWords.contains(toLower(s))), false)
    }
    val metadata = outputSchema(stage.getOutputCol).metadata
    dataset.select(SCol(), tUDF(stage.getOutputCol, SCol(stage.getInputCol)).setSchema(stage.getOutputCol, metadata))
  }

  override def copy(extra: ParamMap): StopWordsRemoverServing = {
    new StopWordsRemoverServing(stage.copy(extra))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema(stage.getInputCol).dataType
    require(inputType.sameType(ArrayType(StringType)),
      s"Input type must be ArrayType(StringType) but got $inputType.")
    SchemaUtils.appendColumn(schema, stage.getOutputCol, inputType, schema(stage.getInputCol).nullable)
  }

  override val uid: String = stage.uid
}

object StopWordsRemoverServing {
  def apply(stage: StopWordsRemover): StopWordsRemoverServing = new StopWordsRemoverServing(stage)
}