package org.apache.spark.ml.feature

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.data._
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap

class StringIndexerServingModel(stage: StringIndexerModel)
  extends ServingModel[StringIndexerServingModel] {

  override def copy(extra: ParamMap): StringIndexerServingModel = {
    new StringIndexerServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    if (!dataset.schema.fieldNames.contains(stage.getInputCol)) {
      logInfo(s"Input column ${stage.getInputCol} does not exist during transformation. " +
        "Skip StringIndexerModel.")
      return dataset
    }
    transformSchema(dataset.schema)

    val filteredLabels = stage.getHandleInvalid match {
      case StringIndexerServingModel.KEEP_INVALID => stage.labels :+ "__unknown"
      case _ => stage.labels
    }

    val metadata = NominalAttribute.defaultAttr
      .withName(stage.getOutputCol).withValues(filteredLabels).toMetadata()
    // If we are skipping invalid records, filter them out.
    val (filteredDataset, keepInvalid) = stage.getHandleInvalid match {
      case StringIndexerServingModel.SKIP_INVALID =>
        val filtererUDF = UDF.make[Boolean, String](label => labelToIndex.contains(label), false)
        (dataset.na.filter(filtererUDF.apply(stage.getInputCol, dataset(stage.getInputCol))), false)
      case _ => (dataset, stage.getHandleInvalid == StringIndexerServingModel.KEEP_INVALID)
    }

    val indexerUDF = UDF.make[Double, String](label => {
      if (label == null) {
        if (keepInvalid) {
          stage.labels.length
        } else {
          throw new SparkException("StringIndexer encountered NULL value. To handle or skip " +
            "NULLS, try setting StringIndexer.handleInvalid.")
        }
      } else {
        if (labelToIndex.contains(label)) {
          labelToIndex(label)
        } else if (keepInvalid) {
          stage.labels.length
        } else {
          throw new SparkException(s"Unseen label: $label.  To handle unseen labels, " +
            s"set Param handleInvalid to ${StringIndexerServingModel.KEEP_INVALID}.")
        }
      }
    }, false)
    filteredDataset.select(SCol(), indexerUDF.apply(stage.getOutputCol, dataset(stage.getInputCol))
      .setSchema(stage.getOutputCol, metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains(stage.getInputCol)) {
      validateAndTransformSchemaImpl(schema)
    } else {
      // If the input column does not exist during transformation, we skip StringIndexerModel.
      schema
    }
  }

  /** Validates and transforms the input schema. */
  def validateAndTransformSchemaImpl(schema: StructType): StructType = {
    val inputColName = stage.getInputCol
    val inputDataType = schema(inputColName).dataType
    require(inputDataType == StringType || inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be either string type or numeric type, " +
        s"but got $inputDataType.")
    val inputFields = schema.fields
    val outputColName = stage.getOutputCol
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    val attr = NominalAttribute.defaultAttr.withName(stage.getOutputCol)
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }

  override val uid: String = stage.uid

  private val labelToIndex: OpenHashMap[String, Double] = {
    val n = stage.labels.length
    val map = new OpenHashMap[String, Double](n)
    var i = 0
    while (i < n) {
      map.update(stage.labels(i), i)
      i += 1
    }
    map
  }

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.inputCol)) {
      val schema = new StructType().add(new StructField(stage.getInputCol, StringType, true))
      println("StringIndexer:   ", schema)
      new SDFrame(rows)(schema)
    } else {
      throw new Exception (s"featuresCol of ${stage} is not defined!")
    }
  }
}

object StringIndexerServingModel {
  private val SKIP_INVALID: String = "skip"
  private val ERROR_INVALID: String = "error"
  private val KEEP_INVALID: String = "keep"
  private val supportedHandleInvalids: Array[String] =
    Array(SKIP_INVALID, ERROR_INVALID, KEEP_INVALID)
  private val frequencyDesc: String = "frequencyDesc"
  private val frequencyAsc: String = "frequencyAsc"
  private val alphabetDesc: String = "alphabetDesc"
  private val alphabetAsc: String = "alphabetAsc"
  private val supportedStringOrderType: Array[String] =
    Array(frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc)
  
  def apply(stage: StringIndexerModel): StringIndexerServingModel = new StringIndexerServingModel(stage)
}