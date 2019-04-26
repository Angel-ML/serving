package org.apache.spark.ml.feature

import java.util

import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.data.{SDFrame, SRow, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.{ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.{DefaultParamsWritable, SchemaUtils}
import org.apache.spark.sql.types._

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

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.inputCol)) {
      val schema = new StructType().add(new StructField(stage.getInputCol, DoubleType, true))
      new SDFrame(rows)(schema)
    } else if(stage.isDefined(stage.inputCols)) {
      val featuresTypes = rows(0).values.map{ feature =>
        feature match {
          case _ : Double => DoubleType
          case _ : String => StringType
          case _ : Integer => IntegerType
          case _ : Vector => new VectorUDT
          case _ : Array[String] => ArrayType(StringType)
        }
      }
      var schema: StructType = null
      val iter = stage.getInputCols.zip(featuresTypes).iterator
      while (iter.hasNext) {
        val (colName, featureType) = iter.next()
        if (schema == null) {
          schema = new StructType().add(new StructField(colName, featureType, true))
        } else {
          schema = schema.add(new StructField(colName, featureType, true))
        }
      }

      new SDFrame(rows)(schema)
    } else {
      throw new Exception (s"inputCol or inputCols of ${stage} is not defined!")
    }
  }

  override def prepareData(feature: util.Map[String, _]): SDFrame = {
    var schema: StructType = null
    val rows = new Array[Any](feature.size())
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
    } else if (stage.isDefined(stage.inputCols)) {
      val featureNames = feature.keySet.toArray
      if (featureNames.size < stage.getInputCols.length) {
        throw new Exception (s"the input cols doesn't match ${stage.getInputCols.length}")
      } else {
        var index = 0
        stage.getInputCols.foreach{ colName =>
          if (!feature.containsKey(colName)) {
            throw new Exception (s"There is not the input col ${colName} in the input data!")
          } else {
            val value = feature.get(colName)
            val valueType = value match {
              case _ : Double => DoubleType
              case _ : String => StringType
              case _ : Integer => IntegerType
              case _ : Vector => new VectorUDT
              case _ : Array[String] => ArrayType(StringType)
            }
            if (schema == null) {
              schema = new StructType().add(new StructField(colName, valueType, true))
            } else {
              schema = schema.add(new StructField(colName, valueType, true))
            }
            rows(index) = value
            index += 1
          }
        }
        new SDFrame(Array(new SRow(rows)))(schema)
      }
    } else {
      throw new Exception (s"inputCol or inputCols of ${stage} is not defined!")
    }
  }
}

object BucketizerServing{
  def apply(stage: Bucketizer): BucketizerServing = new BucketizerServing(stage)
}
