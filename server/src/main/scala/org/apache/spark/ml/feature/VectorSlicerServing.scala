package org.apache.spark.ml.feature

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup}
import org.apache.spark.ml.data.{SDFrame, UDF}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.ml.util.{MetadataUtils, SchemaUtils}
import org.apache.spark.sql.types.StructType

class VectorSlicerServing(stage: VectorSlicer) extends ServingTrans{
  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)

    val inputAttr = AttributeGroup.fromStructField(dataset.schema($(stage.inputCol)))
    inputAttr.numAttributes.foreach { numFeatures =>
      val maxIndex = $(stage.indices).max
      require(maxIndex < numFeatures,
        s"Selected feature index $maxIndex invalid for only $numFeatures input features.")
    }

    // Prepare output attributes
    val inds = getSelectedFeatureIndices(dataset.schema)
    val selectedAttrs: Option[Array[Attribute]] = inputAttr.attributes.map { attrs =>
      inds.map(index => attrs(index))
    }
    val outputAttr = selectedAttrs match {
      case Some(attrs) => new AttributeGroup($(stage.outputCol), attrs)
      case None => new AttributeGroup($(stage.outputCol), inds.length)
    }

    // Select features
    val slicerUDF = UDF.make[Vector, Vector](vec =>
      vec match {
        case features: DenseVector => Vectors.dense(inds.map(features.apply))
        case features: SparseVector => features.slice(inds)
      }
    )
    //todo: whether metadata is necessary
    dataset.withColum(slicerUDF.apply($(stage.outputCol), dataset($(stage.inputCol))))
  }

  override def copy(extra: ParamMap): VectorSlicerServing = {
    new VectorSlicerServing(stage.copy(extra))
  }

  override def transformSchema(schema: StructType): StructType = {
    require($(stage.indices).length > 0 || $(stage.names).length > 0,
      s"VectorSlicer requires that at least one feature be selected.")
    SchemaUtils.checkColumnType(schema, $(stage.inputCol), new VectorUDT)

    if (schema.fieldNames.contains($(stage.outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(stage.outputCol)} already exists.")
    }
    val numFeaturesSelected = $(stage.indices).length + $(stage.names).length
    val outputAttr = new AttributeGroup($(stage.outputCol), numFeaturesSelected)
    val outputFields = schema.fields :+ outputAttr.toStructField()
    StructType(outputFields)
  }

  override val uid: String = stage.uid

  /** Get the feature indices in order: indices, names */
  private def getSelectedFeatureIndices(schema: StructType): Array[Int] = {
    val nameFeatures = MetadataUtils.getFeatureIndicesFromNames(schema($(stage.inputCol)), $(stage.names))
    val indFeatures = $(stage.indices)
    val numDistinctFeatures = (nameFeatures ++ indFeatures).distinct.length
    lazy val errMsg = "VectorSlicer requires indices and names to be disjoint" +
      s" sets of features, but they overlap." +
      s" indices: ${indFeatures.mkString("[", ",", "]")}." +
      s" names: " +
      nameFeatures.zip($(stage.names)).map { case (i, n) => s"$i:$n" }.mkString("[", ",", "]")
    require(nameFeatures.length + indFeatures.length == numDistinctFeatures, errMsg)
    indFeatures ++ nameFeatures
  }
}

object VectorSlicerServing {
  def apply(stage: VectorSlicer): VectorSlicerServing = new VectorSlicerServing(stage)
}