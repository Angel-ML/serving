package org.apache.spark.ml.feature

import java.util

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.data._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuilder

class InteractionServing(stage: Interaction) extends ServingTrans{

  private var value_Type: String = ""

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, logging = true)
    val inputFeatures = stage.getInputCols.map(c => dataset.schema(c))
    val featureEncoders = getFeatureEncoders(inputFeatures, dataset)
    val featureAttrs = getFeatureAttrs(inputFeatures, dataset)

    val interactUDF = UDF.make[Vector, Seq[Any]](row => {
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
        currentEncoder.foreachNonzeroOutput(row(featureIndex), (i, a) => {
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
    }, true)

    val featureCols = inputFeatures.map { f =>
      f.dataType match {
        case DoubleType => dataset(f.name)
        case _: VectorUDT => dataset(f.name)
        case _: NumericType | BooleanType => dataset(f.name)
      }
    }
    dataset.select(SCol(),
      interactUDF.apply(stage.getOutputCol, featureCols:_*).setSchema(stage.getOutputCol, featureAttrs.toMetadata()))
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
  private def getFeatureEncoders(features: Seq[StructField], dataset: SDFrame): Array[FeatureEncoder] = {
    def getNumFeatures(attr: Attribute): Int = {
      attr match {
        case nominal: NominalAttribute =>
          math.max(1, nominal.getNumValues.getOrElse(
            throw new SparkException("Nominal features must have attr numValues defined.")))
        case _ =>
          1  // numeric feature
      }
    }
    lazy val first = dataset.getRow(0)
    val schema = dataset.schema
    features.map { f =>
      val index = schema.fieldIndex(f.name)
      val numFeatures = f.dataType match {
        case _: NumericType | BooleanType =>
          Array(getNumFeatures(Attribute.fromStructField(f)))
        case _: VectorUDT =>
          val group = AttributeGroup.fromStructField(f)
          if (group.attributes.isDefined) {
            val attrs = group.attributes.getOrElse(
              throw new SparkException("Vector attributes must be defined for interaction."))
            attrs.map(getNumFeatures)
          } else {
            (0 until first.get(index).asInstanceOf[Vector].size).map(_ => 1).toArray[Int]
          }
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
  private def getFeatureAttrs(features: Seq[StructField], dataset: SDFrame): AttributeGroup = {
    var featureAttrs: Seq[Attribute] = Nil
    lazy val first = dataset.getRow(0)
    val schema = dataset.schema
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
          val index = schema.fieldIndex(f.name)
          val group = AttributeGroup.fromStructField(f)
          if (group.attributes.isDefined) {
            encodedFeatureAttrs(group.attributes.get, Some(group.name))
          } else {
            val numAttrs = group.numAttributes.getOrElse(first.get(index).asInstanceOf[Vector].size)
            val attr = Array.tabulate(numAttrs)(i => NumericAttribute.defaultAttr.withName(f.name + "_" + i))
            encodedFeatureAttrs(attr, Some(group.name))
          }
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

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if(stage.isDefined(stage.inputCols)) {
      val featuresTypes = rows(0).values.map{ feature =>
        feature match {
          case _ : Double => {
            setValueType("double")
            DoubleType
          }
          case _ : String =>
            setValueType("string")
            StringType
          case _ : Integer =>
            setValueType("int")
            IntegerType
          case _ : Vector =>
            setValueType("double")
            new VectorUDT
          case _ : Array[String] =>
            setValueType("string")
            ArrayType(StringType)
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
              case _ : Double => {
                setValueType("double")
                DoubleType
              }
              case _ : String =>
                setValueType("string")
                StringType
              case _ : Integer =>
                setValueType("int")
                IntegerType
              case _ : Vector =>
                setValueType("double")
                new VectorUDT
              case _ : Array[String] =>
                setValueType("string")
                ArrayType(StringType)
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

  override def valueType(): String = value_Type

  def setValueType(vtype: String): Unit = {
    value_Type = vtype
  }
}

object InteractionServing {
  def apply(stage: Interaction): InteractionServing = new InteractionServing(stage)
}

/**
  * This class performs on-the-fly one-hot encoding of features as you iterate over them. To
  * indicate which input features should be one-hot encoded, an array of the feature counts
  * must be passed in ahead of time.
  *
  * @param numFeatures Array of feature counts for each input feature. For nominal features this
  *                    count is equal to the number of categories. For numeric features the count
  *                    should be set to 1.
  */
class FeatureEncoder(numFeatures: Array[Int]) extends Serializable {
  assert(numFeatures.forall(_ > 0), "Features counts must all be positive.")

  /** The size of the output vector. */
  val outputSize = numFeatures.sum

  /** Precomputed offsets for the location of each output feature. */
  private val outputOffsets = {
    val arr = new Array[Int](numFeatures.length)
    var i = 1
    while (i < arr.length) {
      arr(i) = arr(i - 1) + numFeatures(i - 1)
      i += 1
    }
    arr
  }

  /**
    * Given an input row of features, invokes the specific function for every non-zero output.
    *
    * @param value The row value to encode, either a Double or Vector.
    * @param f The callback to invoke on each non-zero (index, value) output pair.
    */
  def foreachNonzeroOutput(value: Any, f: (Int, Double) => Unit): Unit = value match {
    case d: Integer =>
      assert(numFeatures.length == 1, "DoubleType columns should only contain one feature.")
      val numOutputCols = numFeatures.head
      if (numOutputCols > 1) {
        assert(
          d >= 0.0 && d == d.toInt && d < numOutputCols,
          s"Values from column must be indices, but got $d.")
        f(d.toInt, 1.0)
      } else {
        f(0, d.toDouble)
      }
    case d: Long =>
      assert(numFeatures.length == 1, "DoubleType columns should only contain one feature.")
      val numOutputCols = numFeatures.head
      if (numOutputCols > 1) {
        assert(
          d >= 0.0 && d == d.toInt && d < numOutputCols,
          s"Values from column must be indices, but got $d.")
        f(d.toInt, 1.0)
      } else {
        f(0, d.toDouble)
      }
    case d: Float =>
      assert(numFeatures.length == 1, "DoubleType columns should only contain one feature.")
      val numOutputCols = numFeatures.head
      if (numOutputCols > 1) {
        assert(
          d >= 0.0 && d == d.toInt && d < numOutputCols,
          s"Values from column must be indices, but got $d.")
        f(d.toInt, 1.0)
      } else {
        f(0, d.toDouble)
      }
    case d: Double =>
      assert(numFeatures.length == 1, "DoubleType columns should only contain one feature.")
      val numOutputCols = numFeatures.head
      if (numOutputCols > 1) {
        assert(
          d >= 0.0 && d == d.toInt && d < numOutputCols,
          s"Values from column must be indices, but got $d.")
        f(d.toInt, 1.0)
      } else {
        f(0, d)
      }
    case vec: Vector =>
      assert(numFeatures.length == vec.size,
        s"Vector column size was ${vec.size}, expected ${numFeatures.length}")
      vec.foreachActive { (i, v) =>
        val numOutputCols = numFeatures(i)
        if (numOutputCols > 1) {
          assert(
            v >= 0.0 && v == v.toInt && v < numOutputCols,
            s"Values from column must be indices, but got $v.")
          f(outputOffsets(i) + v.toInt, 1.0)
        } else {
          f(outputOffsets(i), v)
        }
      }
    case null =>
      throw new SparkException("Values to interact cannot be null.")
    case o =>
      throw new SparkException(s"$o of type ${o.getClass.getName} is not supported.")
  }
}