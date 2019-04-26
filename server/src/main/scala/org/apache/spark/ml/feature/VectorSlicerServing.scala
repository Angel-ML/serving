package org.apache.spark.ml.feature

import java.util

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.data.{SDFrame, SRow, UDF}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.ml.util.{MetadataUtils, SchemaUtils}
import org.apache.spark.sql.types.{AnyType, ArrayType, StructField, StructType}

class VectorSlicerServing(stage: VectorSlicer) extends ServingTrans{

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)

    val inputAttr = AttributeGroup.fromStructField(dataset.schema(stage.getInputCol))
    inputAttr.numAttributes.foreach { numFeatures =>
      val maxIndex = stage.getIndices.max
      require(maxIndex < numFeatures,
        s"Selected feature index $maxIndex invalid for only $numFeatures input features.")
    }

    // Prepare output attributes
    val inds = getSelectedFeatureIndices(dataset.schema)
    val selectedAttrs: Option[Array[Attribute]] = inputAttr.attributes.map { attrs =>
      inds.map(index => attrs(index))
    }
    val outputAttr = selectedAttrs match {
      case Some(attrs) => new AttributeGroup(stage.getOutputCol, attrs)
      case None => new AttributeGroup(stage.getOutputCol, inds.length)
    }

    // Select features
    val slicerUDF = UDF.make[Vector, Vector](vec =>
      vec match {
        case features: DenseVector => Vectors.dense(inds.map(features.apply))
        case features: SparseVector => features.slice(inds)
      }, false)
    dataset.withColum(slicerUDF.apply(stage.getOutputCol, dataset(stage.getInputCol))
      .setSchema(stage.getOutputCol, outputAttr.toMetadata()))
  }

  override def copy(extra: ParamMap): VectorSlicerServing = {
    new VectorSlicerServing(stage.copy(extra))
  }

  override def transformSchema(schema: StructType): StructType = {
    require(stage.getIndices.length > 0 || stage.getNames.length > 0,
      s"VectorSlicer requires that at least one feature be selected.")
    SchemaUtils.checkColumnType(schema, stage.getInputCol, new VectorUDT)

    if (schema.fieldNames.contains(stage.getOutputCol)) {
      throw new IllegalArgumentException(s"Output column ${stage.getOutputCol} already exists.")
    }
    val numFeaturesSelected = stage.getIndices.length + stage.getNames.length
    val outputAttr = new AttributeGroup(stage.getOutputCol, numFeaturesSelected)
    val outputFields = schema.fields :+ outputAttr.toStructField()
    StructType(outputFields)
  }

  override val uid: String = stage.uid

  /** Get the feature indices in order: indices, names */
  private def getSelectedFeatureIndices(schema: StructType): Array[Int] = {
    val nameFeatures = MetadataUtils.getFeatureIndicesFromNames(schema(stage.getInputCol), stage.getNames)
    val indFeatures = stage.getIndices
    val numDistinctFeatures = (nameFeatures ++ indFeatures).distinct.length
    lazy val errMsg = "VectorSlicer requires indices and names to be disjoint" +
      s" sets of features, but they overlap." +
      s" indices: ${indFeatures.mkString("[", ",", "]")}." +
      s" names: " +
      nameFeatures.zip(stage.getNames).map { case (i, n) => s"$i:$n" }.mkString("[", ",", "]")
    require(nameFeatures.length + indFeatures.length == numDistinctFeatures, errMsg)
    indFeatures ++ nameFeatures
  }

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.inputCol)) {
      val featureNum = rows(0).get(0).asInstanceOf[Vector].size
      val defaultAttr = NumericAttribute.defaultAttr
      val attrs = (0 until featureNum).map(n => "f" + (n + 1)).toArray.map(defaultAttr.withName)
      val attrGroup = new AttributeGroup(stage.getInputCol, attrs.asInstanceOf[Array[Attribute]])
      val schema =new StructType(Array(attrGroup.toStructField()))
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
        val value = feature.get(stage.getInputCol)
        val featureNum = value.asInstanceOf[Vector].size
        val defaultAttr = NumericAttribute.defaultAttr
        val attrs = (0 until featureNum).map(n => "f" + (n + 1)).toArray.map(defaultAttr.withName)
        val attrGroup = new AttributeGroup(stage.getInputCol, attrs.asInstanceOf[Array[Attribute]])
        val schema =new StructType(Array(attrGroup.toStructField()))
        val rows =  Array(new SRow(Array(value)))
        new SDFrame(rows)(schema)
      }
    } else {
      throw new Exception (s"inputCol or inputCols of ${stage} is not defined!")
    }
  }
}

object VectorSlicerServing {
  def apply(stage: VectorSlicer): VectorSlicerServing = new VectorSlicerServing(stage)
}