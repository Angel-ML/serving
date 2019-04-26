package org.apache.spark.ml.feature

import java.util

import org.apache.spark.ml.data.{SCol, SDFrame, SRow, UDF}
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap

class CountVectorizerServingModel(stage: CountVectorizerModel)
  extends ServingModel[CountVectorizerServingModel] {

  override def copy(extra: ParamMap): CountVectorizerServingModel = {
    new CountVectorizerServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    //todo: broadcast whether is necessary
    val dict = stage.vocabulary.zipWithIndex.toMap
    val minTf = stage.getMinTF
    val vectorizerUDF = UDF.make[Vector, Array[String]](document => {
      val termCounts = new OpenHashMap[Int, Double]
      var tokenCount = 0L
      document.foreach { term =>
        dict.get(term) match {
          case Some(index) => termCounts.changeValue(index, 1.0, _ + 1.0)
          case None => // ignore terms not in the vocabulary
        }
        tokenCount += 1
      }
      val effectiveMinTF = if (minTf >= 1.0) minTf else tokenCount * minTf
      val effectiveCounts = if (stage.getBinary) {
        termCounts.filter(_._2 >= effectiveMinTF).map(p => (p._1, 1.0)).toSeq
      } else {
        termCounts.filter(_._2 >= effectiveMinTF).toSeq
      }

      Vectors.sparse(dict.size, effectiveCounts)
    }, false)
     dataset.withColum(vectorizerUDF.apply(stage.getOutputCol, SCol(stage.getInputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemaImpl(schema)
  }

  /** Validates and transforms the input schema. */
  def validateAndTransformSchemaImpl(schema: StructType): StructType = {
    val typeCandidates = List(new ArrayType(StringType, true), new ArrayType(StringType, false))
    SchemaUtils.checkColumnTypes(schema, stage.getInputCol, typeCandidates)
    SchemaUtils.appendColumn(schema, stage.getOutputCol, new VectorUDT)
  }

  override val uid: String = stage.uid

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.inputCol)) {
      val schema = new StructType().add(new StructField(stage.getInputCol, ArrayType(StringType), true))
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
      } else if (!feature.get(stage.getInputCol).isInstanceOf[Array[String]]) {
        throw new Exception (s"the type of col ${stage.getInputCol} is not Array[String]")
      } else {
        val schema = new StructType().add(new StructField(stage.getInputCol, ArrayType(StringType), true))
        val rows =  Array(new SRow(Array(feature.get(stage.getInputCol))))
        new SDFrame(rows)(schema)
      }
    } else {
      throw new Exception (s"inputCol or inputCols of ${stage} is not defined!")
    }
  }
}

object CountVectorizerServingModel {
  def apply(stage: CountVectorizerModel): CountVectorizerServingModel = new CountVectorizerServingModel(stage)
}