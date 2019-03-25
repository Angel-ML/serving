package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, UDF}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.transformer.ServingTrans
import org.apache.spark.sql.types.{DataType, StructField, StructType}

abstract class UnaryTransformerServing[IN, OUT, T <: UnaryTransformerServing[IN, OUT, T]]
  extends ServingTrans with HasInputCol with HasOutputCol{

  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  protected def createTransformFunc: IN => OUT

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    val transformUDF = UDF.make[OUT, IN](feature => createTransformFunc(feature))
    dataset.withColum(transformUDF.apply($(outputCol), dataset($(inputCol))))
  }

  override def copy(extra: ParamMap): T = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    validateInputType(inputType)
    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), outputDataType, nullable = false)
    StructType(outputFields)
  }

  /**
    * Validates the input type. Throw an exception if it is invalid.
    */
  protected def validateInputType(inputType: DataType): Unit = {}

  /**
    * Returns the data type of the output column.
    */
  protected def outputDataType: DataType
}
