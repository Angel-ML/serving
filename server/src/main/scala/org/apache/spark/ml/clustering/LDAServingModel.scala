package org.apache.spark.ml.clustering

import java.util.Locale

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.mllib.clustering.{LocalLDAModel => OldLocalLDAModel}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}

abstract class LDAServingModel[M <: LDAServingModel[M, T], T <: LDAModel](stage: T) extends ServingModel[M] with LDAParams {
  
  override def transform(dataset: SDFrame): SDFrame = {

    if ((stage.getTopicDistributionCol).nonEmpty) {

      // TODO: Make the transformer natively in ml framework to avoid extra conversion.
      val transformer = oldLocalModel.getTopicDistributionMethod

      val tUDF = UDF.make[Vector, Vector](v => transformer(OldVectors.fromML(v)).asML, false)

      dataset.withColum(tUDF.apply(stage.getTopicDistributionCol,SCol(stage.getFeaturesCol)))
    } else {
      logWarning("LDAModel.transform was called without any output columns. Set an output column" +
        " such as topicDistributionCol to produce results.")
      dataset
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemaImpl(schema)
  }

  /**
    * Validates and transforms the input schema.
    *
    * @param schema input schema
    * @return output schema
    */
  def validateAndTransformSchemaImpl(schema: StructType): StructType = {
    if (isSet(stage.docConcentration)) {
      if (getDocConcentration.length != 1) {
        require(getDocConcentration.length == getK, s"LDA docConcentration was of length" +
          s" ${getDocConcentration.length}, but k = $getK.  docConcentration must be an array of" +
          s" length either 1 (scalar) or k (num topics).")
      }
      getOptimizer.toLowerCase(Locale.ROOT) match {
        case "online" =>
          require(getDocConcentration.forall(_ >= 0),
            "For Online LDA optimizer, docConcentration values must be >= 0.  Found values: " +
              getDocConcentration.mkString(","))
        case "em" =>
          require(getDocConcentration.forall(_ >= 0),
            "For EM optimizer, docConcentration values must be >= 1.  Found values: " +
              getDocConcentration.mkString(","))
      }
    }
    if (isSet(stage.topicConcentration)) {
      getOptimizer.toLowerCase(Locale.ROOT) match {
        case "online" =>
          require(getTopicConcentration >= 0, s"For Online LDA optimizer, topicConcentration" +
            s" must be >= 0.  Found value: $getTopicConcentration")
        case "em" =>
          require(getTopicConcentration >= 0, s"For EM optimizer, topicConcentration" +
            s" must be >= 1.  Found value: $getTopicConcentration")
      }
    }
    SchemaUtils.checkColumnType(schema, stage.getFeaturesCol, new VectorUDT)
    SchemaUtils.appendColumn(schema, stage.getTopicDistributionCol, new VectorUDT)
  }

  def oldLocalModel: OldLocalLDAModel
}
