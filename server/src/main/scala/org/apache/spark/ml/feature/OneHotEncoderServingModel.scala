package org.apache.spark.ml.feature

import java.util

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.data.{SCol, SDFrame, SRow, UDF}
import org.apache.spark.ml.feature.OneHotEncoderModel
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types._

class OneHotEncoderServingModel(stage: OneHotEncoderModel) extends ServingModel[OneHotEncoderServingModel] {

  private val KEEP_INVALID: String = "keep"
  private val ERROR_INVALID: String = "error"

  override def copy(extra: ParamMap): OneHotEncoderServingModel = {
    new OneHotEncoderServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    val transformedSchema = transformSchema(dataset.schema, true)
    val keepInvalid = stage.getHandleInvalid == OneHotEncoderEstimator.KEEP_INVALID
    var output = dataset

    stage.getInputCols.indices.map { idx =>
      val inputColName = stage.getInputCols(idx)
      val outputColName = stage.getOutputCols(idx)

      val outputAttrGroupFromSchema =
        AttributeGroup.fromStructField(transformedSchema(outputColName))

      val metadata = if (outputAttrGroupFromSchema.size < 0) {
        OneHotEncoderCommon.createAttrGroupForAttrNames(outputColName,
          stage.categorySizes(idx), stage.getDropLast, keepInvalid).toMetadata()
      } else {
        outputAttrGroupFromSchema.toMetadata()
      }


      val encoderUDF = UDF.make[Vector, Double, Int]((l, colIdx)=> {
        val keepInvalid = stage.getHandleInvalid == OneHotEncoderEstimator.KEEP_INVALID
        val configedSizes = getConfigedCategorySizes
        val localCategorySizes = stage.categorySizes

        val label = l.asInstanceOf[Double]
        val origCategorySize = localCategorySizes(colIdx)
        // idx: index in vector of the single 1-valued element
        val idx = if (label >= 0 && label < origCategorySize) {
          label
        } else {
          if (keepInvalid) {
            origCategorySize
          } else {
            if (label < 0) {
              throw new SparkException(s"Negative value: $label. Input can't be negative. " +
                s"To handle invalid values, set Param handleInvalid to " +
                s"${OneHotEncoderEstimator.KEEP_INVALID}")
            } else {
              throw new SparkException(s"Unseen value: $label. To handle unseen values, " +
                s"set Param handleInvalid to ${OneHotEncoderEstimator.KEEP_INVALID}.")
            }
          }
        }

        val size = configedSizes(colIdx)
        if (idx.asInstanceOf[Int] < size) {
          Vectors.sparse(size, Array(idx.asInstanceOf[Int]), Array(1.0))
        } else {
          Vectors.sparse(size, Array.empty[Int], Array.empty[Double])
        }
      }, false)

      val idxUDF =UDF.make[Int](() => idx)
      output = output.withColum(idxUDF.apply("idx"))

      output = output.withColum(encoderUDF.apply(outputColName, SCol(inputColName), SCol("idx"))
        .setSchema(outputColName, metadata))
    }
    output
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColNames = stage.getInputCols

    require(inputColNames.length == stage.categorySizes.length,
      s"The number of input columns ${inputColNames.length} must be the same as the number of " +
        s"features ${stage.categorySizes.length} during fitting.")

    val keepInvalid = stage.getHandleInvalid == OneHotEncoderEstimator.KEEP_INVALID
    val transformedSchema = validateAndTransformSchema(schema, dropLast = stage.getDropLast,
      keepInvalid = keepInvalid)
    verifyNumOfValues(transformedSchema)
  }

  override val uid: String = stage.uid

  private def getConfigedCategorySizes: Array[Int] = {
    val dropLast = stage.getDropLast
    val keepInvalid = stage.getHandleInvalid == OneHotEncoderEstimator.KEEP_INVALID

    if (!dropLast && keepInvalid) {
      // When `handleInvalid` is "keep", an extra category is added as last category
      // for invalid data.
      stage.categorySizes.map(_ + 1)
    } else if (dropLast && !keepInvalid) {
      // When `dropLast` is true, the last category is removed.
      stage.categorySizes.map(_ - 1)
    } else {
      // When `dropLast` is true and `handleInvalid` is "keep", the extra category for invalid
      // data is removed. Thus, it is the same as the plain number of categories.
      stage.categorySizes
    }
  }

  protected def validateAndTransformSchema(
                                            schema: StructType,
                                            dropLast: Boolean,
                                            keepInvalid: Boolean): StructType = {
    val inputColNames = stage.getInputCols
    val outputColNames = stage.getOutputCols

    require(inputColNames.length == outputColNames.length,
      s"The number of input columns ${inputColNames.length} must be the same as the number of " +
        s"output columns ${outputColNames.length}.")

    // Input columns must be NumericType.
    inputColNames.foreach(SchemaUtils.checkNumericType(schema, _))

    // Prepares output columns with proper attributes by examining input columns.
    val inputFields = stage.getInputCols.map(schema(_))

    val outputFields = inputFields.zip(outputColNames).map { case (inputField, outputColName) =>
      OneHotEncoderCommon.transformOutputColumnSchema(
        inputField, outputColName, dropLast, keepInvalid)
    }
    outputFields.foldLeft(schema) { case (newSchema, outputField) =>
      SchemaUtils.appendColumn(newSchema, outputField)
    }
  }

  /**
    * If the metadata of input columns also specifies the number of categories, we need to
    * compare with expected category number with `handleInvalid` and `dropLast` taken into
    * account. Mismatched numbers will cause exception.
    */
  private def verifyNumOfValues(schema: StructType): StructType = {
    val configedSizes = getConfigedCategorySizes
    stage.getOutputCols.zipWithIndex.foreach { case (outputColName, idx) =>
      val inputColName = stage.getInputCols(idx)
      val attrGroup = AttributeGroup.fromStructField(schema(outputColName))

      // If the input metadata specifies number of category for output column,
      // comparing with expected category number with `handleInvalid` and
      // `dropLast` taken into account.
      if (attrGroup.attributes.nonEmpty) {
        val numCategories = configedSizes(idx)
        require(attrGroup.size == numCategories, "OneHotEncoderModel expected " +
          s"$numCategories categorical values for input column $inputColName, " +
          s"but the input column had metadata specifying ${attrGroup.size} values.")
      }
    }
    schema
  }

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if(stage.isDefined(stage.inputCols)) {
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
    if (stage.isDefined(stage.inputCols)) {
      val featureNames = feature.keySet.toArray
      if (featureNames.size < stage.getInputCols.length) {
        throw new Exception (s"the input cols doesn't match ${stage.getInputCols.length}")
      } else {
        var index = 0
        stage.getInputCols.foreach{ colName =>
          if (!feature.containsKey(colName)) {
            throw new Exception (s"the ${colName} is not included in the input col(s)")
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

object OneHotEncoderServingModel {
  def apply(stage: OneHotEncoderModel): OneHotEncoderServingModel = new OneHotEncoderServingModel(stage)

  private def genOutputAttrNames(inputCol: StructField): Option[Array[String]] = {
    val inputAttr = Attribute.fromStructField(inputCol)
    inputAttr match {
      case nominal: NominalAttribute =>
        if (nominal.values.isDefined) {
          nominal.values
        } else if (nominal.numValues.isDefined) {
          nominal.numValues.map(n => Array.tabulate(n)(_.toString))
        } else {
          None
        }
      case binary: BinaryAttribute =>
        if (binary.values.isDefined) {
          binary.values
        } else {
          Some(Array.tabulate(2)(_.toString))
        }
      case _: NumericAttribute =>
        throw new RuntimeException(
          s"The input column ${inputCol.name} cannot be continuous-value.")
      case _ =>
        None // optimistic about unknown attributes
    }
  }

  /** Creates an `AttributeGroup` filled by the `BinaryAttribute` named as required. */
  private def genOutputAttrGroup(
                                  outputAttrNames: Option[Array[String]],
                                  outputColName: String): AttributeGroup = {
    outputAttrNames.map { attrNames =>
      val attrs: Array[Attribute] = attrNames.map { name =>
        BinaryAttribute.defaultAttr.withName(name)
      }
      new AttributeGroup(outputColName, attrs)
    }.getOrElse{
      new AttributeGroup(outputColName)
    }
  }

  /** Creates an `AttributeGroup` with the required number of `BinaryAttribute`. */
  def createAttrGroupForAttrNames(
                                   outputColName: String,
                                   numAttrs: Int,
                                   dropLast: Boolean,
                                   keepInvalid: Boolean): AttributeGroup = {
    val outputAttrNames = Array.tabulate(numAttrs)(_.toString)
    val filtered = if (dropLast && !keepInvalid) {
      outputAttrNames.dropRight(1)
    } else if (!dropLast && keepInvalid) {
      outputAttrNames ++ Seq("invalidValues")
    } else {
      outputAttrNames
    }
    genOutputAttrGroup(Some(filtered), outputColName)
  }
}
