package org.apache.spark.ml.feature

import java.util.NoSuchElementException

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.data.{SDFrame, UDF}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.util.SchemaUtils

class VectorIndexerServingModel(stage: VectorIndexerModel) extends ServingModel[VectorIndexerServingModel] {
  private val SKIP_INVALID: String = "skip"
  private val ERROR_INVALID: String = "error"
  private val KEEP_INVALID: String = "keep"

  override def copy(extra: ParamMap): VectorIndexerServingModel = {
    new VectorIndexerServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    //todo:
    val newField = ???
    val transformUDF = UDF.make[Vector, Vector](features => transformFunc(features))
    val ds = dataset.withColum(transformUDF.apply($(stage.outputCol), dataset(${stage.inputCol})))
    if (stage.getHandleInvalid == SKIP_INVALID) {
      ds.na()
    } else {
      ds
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    val dataType = new VectorUDT
    require(isDefined(stage.inputCol),
      s"VectorIndexerModel requires input column parameter: ${stage.inputCol}")
    require(isDefined(stage.outputCol),
      s"VectorIndexerModel requires output column parameter: ${stage.outputCol}")
    SchemaUtils.checkColumnType(schema, $(stage.inputCol), dataType)

    // If the input metadata specifies numFeatures, compare with expected numFeatures.
    val origAttrGroup = AttributeGroup.fromStructField(schema($(stage.inputCol)))
    val origNumFeatures: Option[Int] = if (origAttrGroup.attributes.nonEmpty) {
      Some(origAttrGroup.attributes.get.length)
    } else {
      origAttrGroup.numAttributes
    }
    require(origNumFeatures.forall(_ == stage.numFeatures), "VectorIndexerModel expected" +
      s" ${stage.numFeatures} features, but input column ${$(stage.inputCol)} had metadata specifying" +
      s" ${origAttrGroup.numAttributes.get} features.")

    val newField = prepOutputField(schema)
    val outputFields = schema.fields :+ newField
    StructType(outputFields)
  }

  override val uid: String = stage.uid

  /** Per-vector transform function */
  private lazy val transformFunc: Vector => Vector = {
    val sortedCatFeatureIndices = stage.categoryMaps.keys.toArray.sorted
    val localVectorMap = stage.categoryMaps
    val localNumFeatures = stage.numFeatures
    val localHandleInvalid = stage.getHandleInvalid
    val f: Vector => Vector = { (v: Vector) =>
      assert(v.size == localNumFeatures, "VectorIndexerModel expected vector of length" +
        s" ${stage.numFeatures} but found length ${v.size}")
      v match {
        case dv: DenseVector =>
          var hasInvalid = false
          val tmpv = dv.copy
          localVectorMap.foreach { case (featureIndex: Int, categoryMap: Map[Double, Int]) =>
            try {
              tmpv.values(featureIndex) = categoryMap(tmpv(featureIndex))
            } catch {
              case _: NoSuchElementException =>
                localHandleInvalid match {
                  case ERROR_INVALID =>
                    throw new SparkException(s"VectorIndexer encountered invalid value " +
                      s"${tmpv(featureIndex)} on feature index ${featureIndex}. To handle " +
                      s"or skip invalid value, try setting VectorIndexer.handleInvalid.")
                  case KEEP_INVALID =>
                    tmpv.values(featureIndex) = categoryMap.size
                  case SKIP_INVALID =>
                    hasInvalid = true
                }
            }
          }
          if (hasInvalid) null else tmpv
        case sv: SparseVector =>
          // We use the fact that categorical value 0 is always mapped to index 0.
          var hasInvalid = false
          val tmpv = sv.copy
          var catFeatureIdx = 0 // index into sortedCatFeatureIndices
        var k = 0 // index into non-zero elements of sparse vector
          while (catFeatureIdx < sortedCatFeatureIndices.length && k < tmpv.indices.length) {
            val featureIndex = sortedCatFeatureIndices(catFeatureIdx)
            if (featureIndex < tmpv.indices(k)) {
              catFeatureIdx += 1
            } else if (featureIndex > tmpv.indices(k)) {
              k += 1
            } else {
              try {
                tmpv.values(k) = localVectorMap(featureIndex)(tmpv.values(k))
              } catch {
                case _: NoSuchElementException =>
                  localHandleInvalid match {
                    case ERROR_INVALID =>
                      throw new SparkException(s"VectorIndexer encountered invalid value " +
                        s"${tmpv.values(k)} on feature index ${featureIndex}. To handle " +
                        s"or skip invalid value, try setting VectorIndexer.handleInvalid.")
                    case KEEP_INVALID =>
                      tmpv.values(k) = localVectorMap(featureIndex).size
                    case SKIP_INVALID =>
                      hasInvalid = true
                  }
              }
              catFeatureIdx += 1
              k += 1
            }
          }
          if (hasInvalid) null else tmpv
      }
    }
    f
  }

  /**
    * Prepare the output column field, including per-feature metadata.
    * @param schema  Input schema
    * @return  Output column field.  This field does not contain non-ML metadata.
    */
  private def prepOutputField(schema: StructType): StructField = {
    val origAttrGroup = AttributeGroup.fromStructField(schema($(stage.inputCol)))
    val featureAttributes: Array[Attribute] = if (origAttrGroup.attributes.nonEmpty) {
      // Convert original attributes to modified attributes
      val origAttrs: Array[Attribute] = origAttrGroup.attributes.get
      origAttrs.zip(partialFeatureAttributes).map {
        case (origAttr: Attribute, featAttr: BinaryAttribute) =>
          if (origAttr.name.nonEmpty) {
            featAttr.withName(origAttr.name.get)
          } else {
            featAttr
          }
        case (origAttr: Attribute, featAttr: NominalAttribute) =>
          if (origAttr.name.nonEmpty) {
            featAttr.withName(origAttr.name.get)
          } else {
            featAttr
          }
        case (origAttr: Attribute, featAttr: NumericAttribute) =>
          origAttr.withIndex(featAttr.index.get)
        case (origAttr: Attribute, _) =>
          origAttr
      }
    } else {
      partialFeatureAttributes
    }
    val newAttributeGroup = new AttributeGroup($(stage.outputCol), featureAttributes)
    newAttributeGroup.toStructField()
  }

  /**
    * Pre-computed feature attributes, with some missing info.
    * In transform(), set attribute name and other info, if available.
    */
  private val partialFeatureAttributes: Array[Attribute] = {
    val attrs = new Array[Attribute](stage.numFeatures)
    var categoricalFeatureCount = 0 // validity check for numFeatures, categoryMaps
    var featureIndex = 0
    while (featureIndex < stage.numFeatures) {
      if (stage.categoryMaps.contains(featureIndex)) {
        // categorical feature
        val rawFeatureValues: Array[String] =
          stage.categoryMaps(featureIndex).toArray.sortBy(_._1).map(_._1).map(_.toString)

        val featureValues = if (stage.getHandleInvalid == VectorIndexer.KEEP_INVALID) {
          (rawFeatureValues.toList :+ "__unknown").toArray
        } else {
          rawFeatureValues
        }
        if (featureValues.length == 2 && stage.getHandleInvalid != VectorIndexer.KEEP_INVALID) {
          attrs(featureIndex) = new BinaryAttribute(index = Some(featureIndex),
            values = Some(featureValues))
        } else {
          attrs(featureIndex) = new NominalAttribute(index = Some(featureIndex),
            isOrdinal = Some(false), values = Some(featureValues))
        }
        categoricalFeatureCount += 1
      } else {
        // continuous feature
        attrs(featureIndex) = new NumericAttribute(index = Some(featureIndex))
      }
      featureIndex += 1
    }
    require(categoricalFeatureCount == stage.categoryMaps.size, "VectorIndexerModel given categoryMaps" +
      s" with keys outside expected range [0,...,numFeatures), where numFeatures=${stage.numFeatures}")
    attrs
  }
}

object VectorIndexerServingModel {
  def apply(stage: VectorIndexerModel): VectorIndexerServingModel = new VectorIndexerServingModel(stage)
}
