package org.apache.spark.ml.feature

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.data.{SCol, SDFrame, SRow, UDF}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.unsafe.hash.Murmur3_x86_32._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

import scala.language.implicitConversions
import scala.collection.mutable

class HashingTFServing(stage: HashingTF) extends ServingTrans{

  override def transform(dataset: SDFrame): SDFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val tUDF = UDF.make[Vector, Seq[String]](trans, false)
    val metadata = outputSchema(stage.getOutputCol).metadata
    dataset.select(SCol(), tUDF(stage.getOutputCol, SCol(stage.getInputCol)).setSchema(stage.getOutputCol, metadata))
  }

  override def copy(extra: ParamMap): HashingTFServing = {
    new HashingTFServing(stage.copy(extra))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema(stage.getInputCol).dataType
    require(inputType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType, but got $inputType.")
    val attrGroup = new AttributeGroup(stage.getOutputCol, stage.getNumFeatures)
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  override val uid: String = stage.uid

  private var hashAlgorithm = HashingTFServing.Murmur3

  def trans(document: Iterable[String]): Vector = {
    val termFrequencies = mutable.HashMap.empty[Int, Double]
    val setTF =
      if (stage.getBinary) (i: Int) => 1.0 else (i: Int) => termFrequencies.getOrElse(i, 0.0) + 1.0
    val hashFunc: Any => Int = getHashFunction
    document.foreach { term =>
      val i = Utils.nonNegativeMod(hashFunc(term), stage.getNumFeatures)
      termFrequencies.put(i, setTF(i))
    }
    Vectors.sparse(stage.getNumFeatures, termFrequencies.toSeq)
  }

  private def getHashFunction: Any => Int = hashAlgorithm match {
    case HashingTFServing.Murmur3 =>  HashingTFServing.murmur3Hash
    case HashingTFServing.Native => HashingTFServing.nativeHash
    case _ =>
      // This should never happen.
      throw new IllegalArgumentException(
        s"HashingTF does not recognize hash algorithm $hashAlgorithm")
  }

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.inputCol)) {
      val schema = new StructType().add(new StructField(stage.getInputCol, ArrayType(StringType), true))
      new SDFrame(rows)(schema)
    } else {
      throw new Exception (s"inputCol of ${stage} is not defined!")
    }
  }
}

object HashingTFServing {
  def apply(stage: HashingTF): HashingTFServing = new HashingTFServing(stage)

  private val Native: String = "native"

  private val Murmur3: String = "murmur3"

  private val seed = 42

  private def nativeHash(term: Any): Int = term.##

  /**
    * Calculate a hash code value for the term object using
    * Austin Appleby's MurmurHash 3 algorithm (MurmurHash3_x86_32).
    * This is the default hash algorithm used from Spark 2.0 onwards.
    */
  private def murmur3Hash(term: Any): Int = {
    term match {
      case null => seed
      case b: Boolean => hashInt(if (b) 1 else 0, seed)
      case b: Byte => hashInt(b, seed)
      case s: Short => hashInt(s, seed)
      case i: Int => hashInt(i, seed)
      case l: Long => hashLong(l, seed)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case s: String =>
        val utf8 = UTF8String.fromString(s)
        hashUnsafeBytes(utf8.getBaseObject, utf8.getBaseOffset, utf8.numBytes(), seed)
      case _ => throw new SparkException("HashingTF with murmur3 algorithm does not " +
        s"support type ${term.getClass.getCanonicalName} of input data.")
    }
  }
}