package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

class StopWordsRemoverServing(stage: StopWordsRemover) extends ServingTrans{
  override def transform(dataset: SDFrame): SDFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val tUDF = if ($(stage.caseSensitive)) {
      val stopWordsSet = $(stage.stopWords).toSet
      UDF.make[Seq[String], Seq[String]](terms => terms.filter(s => !stopWordsSet.contains(s)))
    } else {
      // TODO: support user locale (SPARK-15064)
      val toLower = (s: String) => if (s != null) s.toLowerCase else s
      val lowerStopWords = $(stage.stopWords).map(toLower(_)).toSet
      UDF.make[Seq[String], Seq[String]](terms => terms.filter(s => !lowerStopWords.contains(toLower(s))))
    }
    val metadata = outputSchema($(stage.outputCol)).metadata
    //todo: whether select is correct, metadata
    dataset.select(SCol(), tUDF($(stage.outputCol), SCol($(stage.inputCol))))
  }

  override def copy(extra: ParamMap): StopWordsRemoverServing = {
    new StopWordsRemoverServing(stage.copy(extra))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(stage.inputCol)).dataType
    require(inputType.sameType(ArrayType(StringType)),
      s"Input type must be ArrayType(StringType) but got $inputType.")
    SchemaUtils.appendColumn(schema, $(stage.outputCol), inputType, schema($(stage.inputCol)).nullable)
  }

  override val uid: String = stage.uid
}

object StopWordsRemoverServing {
  def apply(stage: StopWordsRemover): StopWordsRemoverServing = new StopWordsRemoverServing(stage)
}