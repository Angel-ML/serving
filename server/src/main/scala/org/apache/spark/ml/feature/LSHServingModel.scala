package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, SRow, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types._

abstract class LSHServingModel[M <: LSHServingModel[M, T], T <: LSHModel[T]](stage: T) extends ServingModel[M] {

  val hashFunction: Vector => Array[Vector]

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    val transformUDF = UDF.make[Array[Vector], Vector](hashFunction, false)
    dataset.withColum(transformUDF.apply(stage.getOutputCol, dataset(stage.getInputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemaImpl(schema)
  }

  def validateAndTransformSchemaImpl(schema: StructType): StructType = {
    SchemaUtils.appendColumn(schema, stage.getOutputCol, DataTypes.createArrayType(new VectorUDT))
  }

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.inputCol)) {
      val featureType = rows(0).get(0) match {
        case _ : Double => DoubleType
        case _ : String => StringType
        case _ : Integer => IntegerType
        case _ : Vector => new VectorUDT
      }
      val schema = new StructType().add(new StructField(stage.getInputCol, featureType, true))
      new SDFrame(rows)(schema)
    } else {
      throw new Exception (s"featuresCol of ${stage} is not defined!")
    }
  }
}
