package org.apache.spark.ml.feature
import org.apache.spark.ml.data.{SDFrame, UDF}
import org.apache.spark.ml.param.ParamMap
import scala.language.implicitConversions
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructType}

class NGramServing(stage: NGram) extends UnaryTransformerServing[Seq[String], Seq[String], NGramServing, NGram](stage) {
  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  protected def createTransformFunc: Seq[String] => Seq[String] = {
    _.iterator.sliding(stage.getN).withPartial(false).map(_.mkString(" ")).toSeq
  }

  override val uid: String = stage.uid

  override def copy(extra: ParamMap): NGramServing = {
    new NGramServing(stage.copy(extra))
  }

  /**
    * Returns the data type of the output column.
    */
 def outputDataType: DataType = new ArrayType(StringType, false)

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    val transformUDF = UDF.make[Seq[String], Seq[String]](createTransformFunc, false)
    dataset.withColum(transformUDF.apply(stage.getOutputCol, dataset(stage.getInputCol)))
  }


  /**
    * Validates the input type. Throw an exception if it is invalid.
    */
  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.sameType(ArrayType(StringType)),
      s"Input type must be ArrayType(StringType) but got $inputType.")
  }
}

object NGramServing {
  def apply(stage: NGram): NGramServing = new NGramServing(stage)
}