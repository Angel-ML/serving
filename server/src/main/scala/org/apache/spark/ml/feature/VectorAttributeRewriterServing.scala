package org.apache.spark.ml.feature

import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.data.{SCol, SDFrame}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.sql.types.StructType

class VectorAttributeRewriterServing(stage: VectorAttributeRewriter) extends ServingTrans{

  override def transform(dataset: SDFrame): SDFrame = {
    val metadata = {
      val group = AttributeGroup.fromStructField(dataset.schema(stage.vectorCol))
      val attrs = group.attributes.get.map { attr =>
        if (attr.name.isDefined) {
          val name = stage.prefixesToRewrite.foldLeft(attr.name.get) { case (curName, (from, to)) =>
            curName.replace(from, to)
          }
          attr.withName(name)
        } else {
          attr
        }
      }
      new AttributeGroup(stage.vectorCol, attrs).toMetadata()
    }
    val otherCols = dataset.columns.filter(_ != stage.vectorCol).map(colName => SCol(colName))
    val rewrittenCol = SCol(stage.vectorCol).setSchema(stage.vectorCol, metadata)
    dataset.select(otherCols :+ rewrittenCol : _*)
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
}

object VectorAttributeRewriterServing {
  def apply(stage: VectorAttributeRewriter): VectorAttributeRewriterServing = new VectorAttributeRewriterServing(stage)
}