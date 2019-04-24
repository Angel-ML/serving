package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SCol, SDFrame, SRow}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.sql.types.StructType

class ColumnPrunerServing(stage: ColumnPruner) extends ServingTrans{

  override def transform(dataset: SDFrame): SDFrame = {
    val columnsToKeep = dataset.columns.filter(!stage.columnsToPrune.contains(_))
    dataset.select(columnsToKeep.map(colName => SCol(colName)): _*)
  }

  override def copy(extra: ParamMap): ServingTrans = {
    new ColumnPrunerServing(stage.copy(extra))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields.filter(col => !stage.columnsToPrune.contains(col.name)))
  }

  override val uid: String = stage.uid

  override def prepareData(rows: Array[SRow]): SDFrame = ??? //todo
}

object ColumnPrunerServing {
  def apply(stage: ColumnPruner): ColumnPrunerServing = new ColumnPrunerServing(stage)
}