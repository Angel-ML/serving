package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, UDF}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.StructType

abstract class LSHServingModel[T <: LSHModel[T]] extends ServingModel[T] with LSHParams{

  val hashFunction: Vector => Array[Vector]

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    val transformUDF = UDF.make[Array[Vector], Vector](feature => hashFunction(feature))
    dataset.withColum(transformUDF.apply($(outputCol), dataset($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}
