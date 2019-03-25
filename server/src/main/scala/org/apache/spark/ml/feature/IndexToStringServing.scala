package org.apache.spark.ml.feature

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.sql.types.{NumericType, StringType, StructField, StructType}

class IndexToStringServing(stage: IndexToString) extends ServingTrans {

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, logging = true)
    val inputColSchema = dataset.schema($(stage.inputCol))
    // If the labels array is empty use column metadata
    val values = if (!isDefined(stage.labels) || $(stage.labels).isEmpty) {
      Attribute.fromStructField(inputColSchema)
        .asInstanceOf[NominalAttribute].values.get
    } else {
      $(stage.labels)
    }
    val indexer = UDF.make[String, Double](index => {
      val idx = index.toInt
      if (0 <= idx && idx < values.length) {
        values(idx)
      } else {
        throw new SparkException(s"Unseen index: $index ??")
      }
    })
    val outputColName = $(stage.outputCol)
    //todo: whether select is correct, cast type
    dataset.select(SCol(), indexer.apply(outputColName, dataset($(stage.inputCol))))
  }

  override def copy(extra: ParamMap): IndexToStringServing = {
    new IndexToStringServing(stage.copy(extra))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(stage.inputCol)
    val inputDataType = schema(inputColName).dataType
    require(inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be a numeric type, " +
        s"but got $inputDataType.")
    val inputFields = schema.fields
    val outputColName = $(stage.outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    val outputFields = inputFields :+ StructField($(stage.outputCol), StringType)
    StructType(outputFields)
  }

  override val uid: String = stage.uid
}

object IndexToStringServing {
  def apply(stage: IndexToString): IndexToStringServing = new IndexToStringServing(stage)
}