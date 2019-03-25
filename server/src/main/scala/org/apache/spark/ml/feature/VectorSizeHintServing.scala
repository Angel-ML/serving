package org.apache.spark.ml.feature

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.data.{SCol, SDFrame, SimpleCol, UDF}
import org.apache.spark.ml.feature.VectorSizeHint
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.linalg._

class VectorSizeHintServing(stage: VectorSizeHint) extends ServingTrans {

  override def transform(dataset: SDFrame): SDFrame = {
    val localInputCol = stage.getInputCol
    val localSize = stage.getSize
    val localHandleInvalid = stage.getHandleInvalid

    val group = AttributeGroup.fromStructField(dataset.schema(localInputCol))
    val newGroup = validateSchemaAndSize(dataset.schema, group)//todo: whether needs
    if (localHandleInvalid == VectorSizeHintServing.OPTIMISTIC_INVALID && group.size == localSize) {
      dataset
    } else {
      val newCol: SCol = localHandleInvalid match {
        case VectorSizeHintServing.OPTIMISTIC_INVALID => SCol(localInputCol)
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
          })
          checkVectorSizeUDF.apply(localInputCol, SCol(localInputCol))
        case VectorSizeHintServing.SKIP_INVALID =>
          val checkVectorSizeUDF = UDF.make[Vector, Vector](vector => {
            if (vector != null && vector.size == localSize) {
              vector
            } else {
              null
            }
          })
          checkVectorSizeUDF.apply(localInputCol, SCol(localInputCol))
      }
      val res = dataset.withColum(newCol)
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
}

object VectorSizeHintServing {
  private val OPTIMISTIC_INVALID = "optimistic"
  private val ERROR_INVALID = "error"
  private val SKIP_INVALID = "skip"

  def apply(stage: VectorSizeHint): VectorSizeHintServing = new VectorSizeHintServing(stage)
}
