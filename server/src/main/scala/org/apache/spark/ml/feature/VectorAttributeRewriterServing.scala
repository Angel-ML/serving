package org.apache.spark.ml.feature

import java.util

import org.apache.spark.ml.data.{SCol, SDFrame, SRow}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.sql.types.StructType

class VectorAttributeRewriterServing(stage: VectorAttributeRewriter) extends ServingTrans{

  override def valueType(): String = ??? //todo???

  override def transform(dataset: SDFrame): SDFrame = {
    val otherCols = dataset.columns.filter(_ != stage.vectorCol).map(colName => SCol(colName))
    val rewrittenCol = SCol(stage.vectorCol)
    dataset.select((otherCols :+ rewrittenCol):_*)
  }

  override def copy(extra: ParamMap): ServingTrans = {
    new VectorAttributeRewriterServing(stage.copy(extra))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      schema.fields.filter(_.name != stage.vectorCol) ++
        schema.fields.filter(_.name == stage.vectorCol))
  }

  override val uid: String = stage.uid

  override def prepareData(rows: Array[SRow]): SDFrame = ??? //todo

  override def prepareData(feature: util.Map[String, _]): SDFrame = ??? //todo
}

object VectorAttributeRewriterServing {
  def apply(stage: VectorAttributeRewriter): VectorAttributeRewriterServing = new VectorAttributeRewriterServing(stage)
}