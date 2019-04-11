package org.apache.spark.ml.feature

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.sql.types.{NumericType, StringType, StructField, StructType}

class IndexToStringServing(stage: IndexToString) extends ServingTrans{

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, logging = true)
    val inputColSchema = dataset.schema(stage.getInputCol)
    // If the labels array is empty use column metadata
    val values = if (!stage.isDefined(stage.labels) || stage.getLabels.isEmpty) {
      println(!stage.isDefined(stage.labels))
      Attribute.fromStructField(inputColSchema)
        .asInstanceOf[NominalAttribute].values.get
    } else {
      stage.getLabels
    }
    val indexer = UDF.make[String, Double](index => {
      val idx = index.toInt
      if (0 <= idx && idx < values.length) {
        values(idx)
      } else {
        throw new SparkException(s"Unseen index: $index ??")
      }
    }, false)
    val outputColName = stage.getOutputCol
    dataset.select(SCol(), indexer.apply(outputColName, dataset(stage.getInputCol)))
  }

  override def copy(extra: ParamMap): IndexToStringServing = {
    new IndexToStringServing(stage.copy(extra))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColName = stage.getInputCol
    val inputDataType = schema(inputColName).dataType
    require(inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be a numeric type, " +
        s"but got $inputDataType.")
    val inputFields = schema.fields
    val outputColName = stage.getOutputCol
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    val outputFields = inputFields :+ StructField(stage.getOutputCol, StringType)
    StructType(outputFields)
  }

  override val uid: String = stage.uid
}

object IndexToStringServing {
  def apply(stage: IndexToString): IndexToStringServing = new IndexToStringServing(stage)
}