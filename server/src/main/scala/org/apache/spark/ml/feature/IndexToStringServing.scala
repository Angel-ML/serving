package org.apache.spark.ml.feature

import java.util

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.data.{SCol, SDFrame, SRow, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.sql.types._

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

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.inputCol)) {
      val schema = new StructType().add(new StructField(stage.getInputCol, DoubleType, true))
      new SDFrame(rows)(schema)
    } else {
      throw new Exception (s"inputCol or inputCols of ${stage} is not defined!")
    }
  }

  override def prepareData(feature: util.Map[String, _]): SDFrame = {
    if (stage.isDefined(stage.inputCol)) {
      val featureName = feature.keySet.toArray
      if (!featureName.contains(stage.getInputCol)) {
        throw new Exception (s"the ${stage.getInputCol} is not included in the input col(s)")
      } else if (!feature.get(stage.getInputCol).isInstanceOf[Double]) {
        throw new Exception (s"the type of col ${stage.getInputCol} is not Double")
      } else {
        val schema = new StructType().add(new StructField(stage.getInputCol, DoubleType, true))
        val rows =  Array(new SRow(Array(feature.get(stage.getInputCol))))
        new SDFrame(rows)(schema)
      }
    } else {
      throw new Exception (s"inputCol or inputCols of ${stage} is not defined!")
    }
  }
}

object IndexToStringServing {
  def apply(stage: IndexToString): IndexToStringServing = new IndexToStringServing(stage)
}