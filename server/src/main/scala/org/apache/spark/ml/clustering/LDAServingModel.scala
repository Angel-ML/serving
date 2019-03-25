package org.apache.spark.ml.clustering

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.types.StructType
import org.apache.spark.mllib.clustering.{LocalLDAModel => OldLocalLDAModel}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}

abstract class LDAServingModel extends ServingModel[LDAServingModel] with LDAParams {
  
  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    //todo:???
    if ($(topicDistributionCol).nonEmpty) {

      // TODO: Make the transformer natively in ml framework to avoid extra conversion.
      val transformer = oldLocalModel.getTopicDistributionMethod

      val tUDF = UDF.make[Vector, Vector](v => transformer(OldVectors.fromML(v)).asML)

      dataset.withColum(tUDF.apply("topicDistribution",SCol("features")))
    } else {
      logWarning("LDAModel.transform was called without any output columns. Set an output column" +
        " such as topicDistributionCol to produce results.")
      dataset
    }
  }

  override def transformSchema(schema: StructType): StructType = ???

  def oldLocalModel: OldLocalLDAModel
}
