package org.apache.spark.ml.feature

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.data.{SCol, SDFrame, SRow, UDF}
import org.apache.spark.ml.feature.{FeatureEncoder, Interaction}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuilder

class InteractionServing(stage: Interaction) extends ServingTrans{

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, logging = true)
    val inputFeatures = stage.getInputCols.map(c => dataset.schema(c))
    val featureEncoders = getFeatureEncoders(inputFeatures)
    val featureAttrs = getFeatureAttrs(inputFeatures)

    val interactUDF = UDF.make[Vector, SRow](row => {
      var indices = ArrayBuilder.make[Int]
      var values = ArrayBuilder.make[Double]
      var size = 1
      indices += 0
      values += 1.0
      var featureIndex = row.length - 1
      while (featureIndex >= 0) {
        val prevIndices = indices.result()
        val prevValues = values.result()
        val prevSize = size
        val currentEncoder = featureEncoders(featureIndex)
        indices = ArrayBuilder.make[Int]
        values = ArrayBuilder.make[Double]
        size *= currentEncoder.outputSize
        currentEncoder.foreachNonzeroOutput(row.get(featureIndex), (i, a) => {
          var j = 0
          while (j < prevIndices.length) {
            indices += prevIndices(j) + i * prevSize
            values += prevValues(j) * a
            j += 1
          }
        })
        featureIndex -= 1
      }
      Vectors.sparse(size, indices.result(), values.result()).compressed
    })

    val featureCols = inputFeatures.map { f =>
      f.dataType match {
        case DoubleType => dataset(f.name)
        case _: VectorUDT => dataset(f.name)
        case _: NumericType | BooleanType => dataset(f.name)
      }
    }
    dataset.select(SCol(),
      interactUDF.apply(stage.getOutputCol, featureCols:_*).setSchema(stage.getOutputCol, featureAttrs.toMetadata()))//todo: struct
  }

  override def copy(extra: ParamMap): InteractionServing = {
    new InteractionServing(stage.copy(extra))
  }

  override def transformSchema(schema: StructType): StructType = {
//    require(get(stage.inputCols).isDefined, "Input cols must be defined first.")
//    require(get(stage.outputCol).isDefined, "Output col must be defined first.")
    require(stage.getInputCols.length > 0, "Input cols must have non-zero length.")
    require(stage.getInputCols.distinct.length == stage.getInputCols.length, "Input cols must be distinct.")
    StructType(schema.fields :+ StructField(stage.getOutputCol, new VectorUDT, false))
  }

  override val uid: String = stage.uid

  /**
    * Creates a feature encoder for each input column, which supports efficient iteration over
    * one-hot encoded feature values. See also the class-level comment of [[FeatureEncoder]].
    *
    * @param features The input feature columns to create encoders for.
    */
  private def getFeatureEncoders(features: Seq[StructField]): Array[FeatureEncoder] = {
    def getNumFeatures(attr: Attribute): Int = {
      attr match {
        case nominal: NominalAttribute =>
          math.max(1, nominal.getNumValues.getOrElse(
            throw new SparkException("Nominal features must have attr numValues defined.")))
        case _ =>
          1  // numeric feature
      }
    }
    features.map { f =>
      val numFeatures = f.dataType match {
        case _: NumericType | BooleanType =>
          Array(getNumFeatures(Attribute.fromStructField(f)))
        case _: VectorUDT =>
          val attrs = AttributeGroup.fromStructField(f).attributes.getOrElse(
            throw new SparkException("Vector attributes must be defined for interaction."))
          attrs.map(getNumFeatures)
      }
      new FeatureEncoder(numFeatures)
    }.toArray
  }

  /**
    * Generates ML attributes for the output vector of all feature interactions. We make a best
    * effort to generate reasonable names for output features, based on the concatenation of the
    * interacting feature names and values delimited with `_`. When no feature name is specified,
    * we fall back to using the feature index (e.g. `foo:bar_2_0` may indicate an interaction
    * between the numeric `foo` feature and a nominal third feature from column `bar`.
    *
    * @param features The input feature columns to the Interaction transformer.
    */
  private def getFeatureAttrs(features: Seq[StructField]): AttributeGroup = {
    var featureAttrs: Seq[Attribute] = Nil
    features.reverse.foreach { f =>
      val encodedAttrs = f.dataType match {
        case _: NumericType | BooleanType =>
          val attr = Attribute.decodeStructField(f, preserveName = true)
          if (attr == UnresolvedAttribute) {
            encodedFeatureAttrs(Seq(NumericAttribute.defaultAttr.withName(f.name)), None)
          } else if (!attr.name.isDefined) {
            encodedFeatureAttrs(Seq(attr.withName(f.name)), None)
          } else {
            encodedFeatureAttrs(Seq(attr), None)
          }
        case _: VectorUDT =>
          val group = AttributeGroup.fromStructField(f)
          encodedFeatureAttrs(group.attributes.get, Some(group.name))
      }
      if (featureAttrs.isEmpty) {
        featureAttrs = encodedAttrs
      } else {
        featureAttrs = encodedAttrs.flatMap { head =>
          featureAttrs.map { tail =>
            NumericAttribute.defaultAttr.withName(head.name.get + ":" + tail.name.get)
          }
        }
      }
    }
    new AttributeGroup(stage.getOutputCol, featureAttrs.toArray)
  }

  /**
    * Generates the output ML attributes for a single input feature. Each output feature name has
    * up to three parts: the group name, feature name, and category name (for nominal features),
    * each separated by an underscore.
    *
    * @param inputAttrs The attributes of the input feature.
    * @param groupName Optional name of the input feature group (for Vector type features).
    */
  private def encodedFeatureAttrs(
                                   inputAttrs: Seq[Attribute],
                                   groupName: Option[String]): Seq[Attribute] = {

    def format(
                index: Int,
                attrName: Option[String],
                categoryName: Option[String]): String = {
      val parts = Seq(groupName, Some(attrName.getOrElse(index.toString)), categoryName)
      parts.flatten.mkString("_")
    }

    inputAttrs.zipWithIndex.flatMap {
      case (nominal: NominalAttribute, i) =>
        if (nominal.values.isDefined) {
          nominal.values.get.map(
            v => BinaryAttribute.defaultAttr.withName(format(i, nominal.name, Some(v))))
        } else {
          Array.tabulate(nominal.getNumValues.get)(
            j => BinaryAttribute.defaultAttr.withName(format(i, nominal.name, Some(j.toString))))
        }
      case (a: Attribute, i) =>
        Seq(NumericAttribute.defaultAttr.withName(format(i, a.name, None)))
    }
  }
}

object InteractionServing {
  def apply(stage: Interaction): InteractionServing = new InteractionServing(stage)
}