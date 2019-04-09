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
  with HasInputCols with HasOutputCols{

  override def copy(extra: ParamMap): BucketizerServing = {
    new BucketizerServing(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    val transformedSchema = transformSchema(dataset.schema)

    val (inputColumns, outputColumns) = if (isSet(stage.inputCols)) {
      (stage.getInputCols.toSeq, stage.getOutputCols.toSeq)
    } else {
      (Seq(stage.getInputCol), Seq(stage.getOutputCol))
    }

    val (filteredDataset, keepInvalid) = {
      if (stage.getHandleInvalid == Bucketizer.SKIP_INVALID) {
        // "skip" NaN option is set, will filter out NaN values in the dataset
        (dataset.na(), false)
      } else {
        (dataset, stage.getHandleInvalid == Bucketizer.KEEP_INVALID)
      }
    }

    val seqOfSplits = if (isSet(stage.inputCols)) {
      stage.getSplitsArray.toSeq
    } else {
      Seq(stage.getSplits)
    }

    val bucketizers: Seq[UDF] = seqOfSplits.zipWithIndex.map { case (splits, idx) =>
      UDF.make[Double, Double](feature => Bucketizer.binarySearchForBuckets(splits, feature, keepInvalid), false)
    }

    var output = dataset
    inputColumns.zipWithIndex.map { case (inputCol, idx) =>
      output = output.withColum(bucketizers(idx)(outputColumns(idx), filteredDataset(inputCol))
        .setSchema(outputColumns(idx), transformedSchema(outputColumns(idx)).metadata))
    }
    output
  }

  override def transformSchema(schema: StructType): StructType = {
    ParamValidators.checkSingleVsMultiColumnParams(stage, Seq(stage.outputCol, stage.splits),
      Seq(stage.outputCols, stage.splitsArray))

    if (isSet(stage.inputCols)) {
      require(stage.getInputCols.length == stage.getOutputCols.length &&
        stage.getInputCols.length == stage.getSplitsArray.length, s"Bucketizer $stage has mismatched Params " +
        s"for multi-column transform.  Params (inputCols, outputCols, splitsArray) should have " +
        s"equal lengths, but they have different lengths: " +
        s"(${stage.getInputCols.length}, ${stage.getOutputCols.length}, ${stage.getSplitsArray.length}).")

      var transformedSchema = schema
      stage.getInputCols.zip(stage.getOutputCols).zipWithIndex.foreach { case ((inputCol, outputCol), idx) =>
        SchemaUtils.checkNumericType(transformedSchema, inputCol)
        transformedSchema = SchemaUtils.appendColumn(transformedSchema,
          prepOutputField(stage.getSplitsArray(idx), outputCol))
      }
      transformedSchema
    } else {
      SchemaUtils.checkNumericType(schema, stage.getInputCol)
      SchemaUtils.appendColumn(schema, prepOutputField(stage.getSplits, stage.getOutputCol))
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
