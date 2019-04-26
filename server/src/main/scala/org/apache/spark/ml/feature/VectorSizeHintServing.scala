package org.apache.spark.ml.feature

import java.util

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.data._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg._

class VectorSizeHintServing(stage: VectorSizeHint) extends ServingTrans {

  override def transform(dataset: SDFrame): SDFrame = {
    val localInputCol = stage.getInputCol
    val localSize = stage.getSize
    val localHandleInvalid = stage.getHandleInvalid

    val group = AttributeGroup.fromStructField(dataset.schema(localInputCol))
    val newGroup = validateSchemaAndSize(dataset.schema, group)
    if (localHandleInvalid == VectorSizeHintServing.OPTIMISTIC_INVALID && group.size == localSize) {
      dataset
    } else {
      val newCol: SCol = localHandleInvalid match {
        case VectorSizeHintServing.OPTIMISTIC_INVALID => {
          val inputUDF = UDF.make[Vector, Vector](input => input, false)
          inputUDF.apply(localInputCol, SCol(localInputCol))
        }
        case VectorSizeHintServing.ERROR_INVALID =>
          val checkVectorSizeUDF = UDF.make[Vector, Vector](vector => {
            if (vector == null) {
              throw new SparkException(s"Got null vector in VectorSizeHint, set `handleInvalid` " +
                s"to 'skip' to filter invalid rows.")
            }
            if (vector.size != localSize) {
              throw new SparkException(s"VectorSizeHint Expecting a vector of size $localSize but" +
                s" got ${vector.size}")
            }
            vector
          }, false)
          checkVectorSizeUDF.apply(localInputCol, SCol(localInputCol))
        case VectorSizeHintServing.SKIP_INVALID =>
          val checkVectorSizeUDF = UDF.make[Vector, Vector](vector => {
            if (vector != null && vector.size == localSize) {
              vector
            } else {
              null
            }
          }, false)
          checkVectorSizeUDF.apply(localInputCol, SCol(localInputCol))
      }
      val res = dataset.withColum(newCol.setSchema(localInputCol, newGroup.toMetadata()).asInstanceOf[UDFCol])
      if (localHandleInvalid == VectorSizeHintServing.SKIP_INVALID) {
        res.na
      } else {
        res
      }
    }
  }

  override def copy(extra: ParamMap): VectorSizeHintServing = {
    new VectorSizeHintServing(stage.copy(extra))
  }

  override def transformSchema(schema: StructType): StructType = {
    val fieldIndex = schema.fieldIndex(stage.getInputCol)
    val fields = schema.fields.clone()
    val inputField = fields(fieldIndex)
    val group = AttributeGroup.fromStructField(inputField)
    val newGroup = validateSchemaAndSize(schema, group)
    fields(fieldIndex) = inputField.copy(metadata = newGroup.toMetadata())
    StructType(fields)
  }

  override val uid: String = stage.uid

  /**
    * Checks that schema can be updated with new size and returns a new attribute group with
    * updated size.
    */
  private def validateSchemaAndSize(schema: StructType, group: AttributeGroup): AttributeGroup = {
    // This will throw a NoSuchElementException if params are not set.
    val localSize = stage.getSize
    val localInputCol = stage.getInputCol

    val inputColType = schema(stage.getInputCol).dataType
    require(
      inputColType.isInstanceOf[VectorUDT],
      s"Input column, ${stage.getInputCol} must be of Vector type, got $inputColType"
    )
    group.size match {
      case `localSize` => group
      case -1 => new AttributeGroup(localInputCol, localSize)
      case _ =>
        val msg = s"Trying to set size of vectors in `$localInputCol` to $localSize but size " +
          s"already set to ${group.size}."
        throw new IllegalArgumentException(msg)
    }
  }

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.inputCol)) {
      val schema = new StructType().add(new StructField(stage.getInputCol, new VectorUDT, true))
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
      } else if (!feature.get(stage.getInputCol).isInstanceOf[Vector]) {
        throw new Exception (s"the type of col ${stage.getInputCol} is not Vector")
      } else {
        val schema = new StructType().add(new StructField(stage.getInputCol, new VectorUDT, true))
        val rows =  Array(new SRow(Array(feature.get(stage.getInputCol))))
        new SDFrame(rows)(schema)
      }
    } else {
      throw new Exception (s"inputCol or inputCols of ${stage} is not defined!")
    }
  }
}

object VectorSizeHintServing {
  private val OPTIMISTIC_INVALID = "optimistic"
  private val ERROR_INVALID = "error"
  private val SKIP_INVALID = "skip"

  def apply(stage: VectorSizeHint): VectorSizeHintServing = new VectorSizeHintServing(stage)
}
