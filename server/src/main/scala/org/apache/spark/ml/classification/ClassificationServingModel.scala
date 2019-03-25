package org.apache.spark.ml.classification

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.feature.PredictionServingModel

abstract class ClassificationServingModel[FeaturesType, M <: ClassificationServingModel[FeaturesType, M]]
  extends PredictionServingModel[FeaturesType, M] with ClassifierParams{

    override def predict(features: FeaturesType): Double = raw2prediction(predictRaw(features))

    override def transform(dataset: SDFrame): SDFrame = {
      transformSchema(dataset.schema)

      var outputData = dataset
      var numColsOutput = 0
      if (getRawPredictionCol != "") {
        val predictRawUDF = UDF.make[Vector, Any](features =>
          predictRaw(features.asInstanceOf[FeaturesType]))
        outputData = outputData.withColum(predictRawUDF.apply(getRawPredictionCol, SCol(getFeaturesCol)))
        numColsOutput += 1
      }
      if (getPredictionCol != "") {
        val predUDF = if (getRawPredictionCol != "") {
          UDF.make[Double, Vector](features => raw2prediction(features))
            .apply(getPredictionCol, SCol(getRawPredictionCol))
        } else {
          UDF.make[Double, Any](features => predict(features.asInstanceOf[FeaturesType]))
            .apply(getPredictionCol,SCol(getFeaturesCol))
        }
        outputData = outputData.withColum(predUDF)
        numColsOutput += 1
      }

      if (numColsOutput == 0) {
        logWarning(s"$uid: ClassificationModel.transform() was called as NOOP" +
          " since no output columns were set.")
      }
      outputData
    }

    def predictRaw(features: FeaturesType): Vector

    def raw2prediction(rawPrediction: Vector): Double = rawPrediction.argmax

    def numClasses: Int

  }
