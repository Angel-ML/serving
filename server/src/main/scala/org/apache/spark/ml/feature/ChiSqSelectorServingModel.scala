package org.apache.spark.ml.feature

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute}
import org.apache.spark.ml.data.{SCol, SDFrame, SRow, UDF}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.util.SchemaUtils

import scala.collection.mutable.ArrayBuilder

class ChiSqSelectorServingModel(stage: ChiSqSelectorModel) extends ServingModel[ChiSqSelectorServingModel] {

  override def copy(extra: ParamMap): ChiSqSelectorServingModel = {
    new ChiSqSelectorServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    val transformedSchema = transformSchema(dataset.schema, true)
    val metadata = transformedSchema.last.metadata

    val selectorUDF = UDF.make[Vector, Vector](compress, false)
    dataset.withColum(selectorUDF.apply(stage.getOutputCol, SCol(stage.getFeaturesCol))
      .setSchema(stage.getOutputCol, metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, stage.getFeaturesCol, new VectorUDT)
    val newField = prepOutputField(schema)
    val outputFields = schema.fields :+ newField
    StructType(outputFields)
  }

  override val uid: String = stage.uid

  /**
    * Returns a vector with features filtered.
    * Preserves the order of filtered features the same as their indices are stored.
    * Might be moved to Vector as .slice
    * @param features vector
    */
  private def compress(features: Vector): Vector = {
    val filterIndices = stage.selectedFeatures.sorted
    features match {
      case SparseVector(size, indices, values) =>
        val newSize = filterIndices.length
        val newValues = new ArrayBuilder.ofDouble
        val newIndices = new ArrayBuilder.ofInt
        var i = 0
        var j = 0
        var indicesIdx = 0
        var filterIndicesIdx = 0
        while (i < indices.length && j < filterIndices.length) {
          indicesIdx = indices(i)
          filterIndicesIdx = filterIndices(j)
          if (indicesIdx == filterIndicesIdx) {
            newIndices += j
            newValues += values(i)
            j += 1
            i += 1
          } else {
            if (indicesIdx > filterIndicesIdx) {
              j += 1
            } else {
              i += 1
            }
          }
        }
        // TODO: Sparse representation might be ineffective if (newSize ~= newValues.size)
        Vectors.sparse(newSize, newIndices.result(), newValues.result())
      case DenseVector(values) =>
        val values = features.toArray
        Vectors.dense(filterIndices.map(i => values(i)))
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }

  /**
    * Prepare the output column field, including per-feature metadata.
    */
  private def prepOutputField(schema: StructType): StructField = {
    val selector = stage.selectedFeatures.toSet
    val origAttrGroup = AttributeGroup.fromStructField(schema(stage.getFeaturesCol))
    val featureAttributes: Array[Attribute] = if (origAttrGroup.attributes.nonEmpty) {
      origAttrGroup.attributes.get.zipWithIndex.filter(x => selector.contains(x._2)).map(_._1)
    } else {
      Array.fill[Attribute](selector.size)(NominalAttribute.defaultAttr)
    }
    val newAttributeGroup = new AttributeGroup(stage.getOutputCol, featureAttributes)
    newAttributeGroup.toStructField()
  }

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.featuresCol)) {
      val schema = new StructType().add(new StructField(stage.getFeaturesCol, new VectorUDT, true))
      new SDFrame(rows)(schema)
    } else {
      throw new Exception (s"featuresCol of ${stage} is not defined!")
    }
  }
}

object ChiSqSelectorServingModel {
  def apply(stage: ChiSqSelectorModel): ChiSqSelectorServingModel = new ChiSqSelectorServingModel(stage)
}