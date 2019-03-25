package org.apache.spark.ml.feature

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.data.{SCol, SDFrame, SimpleCol, UDF}
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.collection.OpenHashMap

class StringIndexerServingModel(stage: StringIndexerModel)
  extends ServingModel[StringIndexerServingModel] with StringIndexerBase {

  override def copy(extra: ParamMap): StringIndexerServingModel = {
    new StringIndexerServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    if (!dataset.schema.fieldNames.contains(${stage.inputCol})) {
      logInfo(s"Input column ${$(stage.inputCol)} does not exist during transformation. " +
        "Skip StringIndexerModel.")
      return dataset
    }
    transformSchema(dataset.schema)

    val filteredLabels = stage.getHandleInvalid match {
      case StringIndexerServingModel.KEEP_INVALID => stage.labels :+ "__unknown"
      case _ => stage.labels
    }

    val metadata = NominalAttribute.defaultAttr
      .withName($(stage.outputCol)).withValues(filteredLabels).toMetadata()
    // If we are skipping invalid records, filter them out.
    val (filteredDataset, keepInvalid) = stage.getHandleInvalid match {
      case StringIndexerServingModel.SKIP_INVALID =>
        val filtererUDF = UDF.make[Boolean, String](label => labelToIndex.contains(label))
        (dataset.na.filter(filtererUDF.apply($(stage.inputCol), dataset($(stage.inputCol)))), false)
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
    })
    filteredDataset.select(SCol(), indexerUDF($(stage.outputCol), dataset($(stage.inputCol))))//todo:cast
  }

  override def transformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains($(inputCol))) {
      validateAndTransformSchema(schema)
    } else {
      // If the input column does not exist during transformation, we skip StringIndexerModel.
      schema
    }
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