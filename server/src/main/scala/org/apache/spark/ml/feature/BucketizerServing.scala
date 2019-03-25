package org.apache.spark.ml.feature

import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.data.{SDFrame, UDF}
import org.apache.spark.ml.param.{ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.{DefaultParamsWritable, SchemaUtils}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

class BucketizerServing(stage: Bucketizer) extends ServingModel[BucketizerServing]
  with HasHandleInvalid with HasInputCol with HasOutputCol
  with HasInputCols with HasOutputCols with DefaultParamsWritable {

  override def copy(extra: ParamMap): BucketizerServing = {
    new BucketizerServing(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    val transformedSchema = transformSchema(dataset.schema)

    val (inputColumns, outputColumns) = if (isSet(inputCols)) {
      ($(inputCols).toSeq, $(outputCols).toSeq)
    } else {
      (Seq($(inputCol)), Seq($(outputCol)))
    }

    val (filteredDataset, keepInvalid) = {
      if (getHandleInvalid == Bucketizer.SKIP_INVALID) {
        // "skip" NaN option is set, will filter out NaN values in the dataset
        (dataset.na(), false)
      } else {
        (dataset, getHandleInvalid == Bucketizer.KEEP_INVALID)
      }
    }

    val seqOfSplits = if (isSet(inputCols)) {
      $(stage.splitsArray).toSeq
    } else {
      Seq($(stage.splits))
    }

    val bucketizers: Seq[UDF] = seqOfSplits.zipWithIndex.map { case (splits, idx) =>
      UDF.make[Double, Double](feature => Bucketizer.binarySearchForBuckets(splits, feature, keepInvalid))
    }

    inputColumns.zipWithIndex.map { case (inputCol, idx) =>
      filteredDataset.withColum(bucketizers(idx)(outputColumns(idx), filteredDataset(inputCol)))
    }

    filteredDataset
  }

  override def transformSchema(schema: StructType): StructType = {
    ParamValidators.checkSingleVsMultiColumnParams(this, Seq(outputCol, stage.splits),
      Seq(outputCols, stage.splitsArray))

    if (isSet(inputCols)) {
      require(getInputCols.length == getOutputCols.length &&
        getInputCols.length == stage.getSplitsArray.length, s"Bucketizer $this has mismatched Params " +
        s"for multi-column transform.  Params (inputCols, outputCols, splitsArray) should have " +
        s"equal lengths, but they have different lengths: " +
        s"(${getInputCols.length}, ${getOutputCols.length}, ${stage.getSplitsArray.length}).")

      var transformedSchema = schema
      $(inputCols).zip($(outputCols)).zipWithIndex.foreach { case ((inputCol, outputCol), idx) =>
        SchemaUtils.checkNumericType(transformedSchema, inputCol)
        transformedSchema = SchemaUtils.appendColumn(transformedSchema,
          prepOutputField($(stage.splitsArray)(idx), outputCol))
      }
      transformedSchema
    } else {
      SchemaUtils.checkNumericType(schema, $(inputCol))
      SchemaUtils.appendColumn(schema, prepOutputField($(stage.splits), $(outputCol)))
    }
  }

  override val uid: String = stage.uid

  private def prepOutputField(splits: Array[Double], outputCol: String): StructField = {
    val buckets = splits.sliding(2).map(bucket => bucket.mkString(", ")).toArray
    val attr = new NominalAttribute(name = Some(outputCol), isOrdinal = Some(true),
      values = Some(buckets))
    attr.toStructField()
  }
}

object BucketizerServing{
  def apply(stage: Bucketizer): BucketizerServing = new BucketizerServing(stage)
}
