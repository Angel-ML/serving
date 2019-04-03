package org.apache.spark.ml.classification

import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.linalg._

abstract class ClassificationServingModel[FeaturesType, M <: ClassificationServingModel[FeaturesType, M, T],
  T <: ClassificationModel[FeaturesType, T]](stage: T)
  extends PredictionServingModel[FeaturesType, M, T](stage) with ClassifierParams {

    override def predict(features: Vector): Double = raw2prediction(predictRaw(features))

    override def transform(dataset: SDFrame): SDFrame = {
      transformSchema(dataset.schema, true)

      var outputData = dataset
      var numColsOutput = 0
      if (stage.getRawPredictionCol != "") {
        val predictRawUDF = UDF.make[Vector, Vector](features =>
          predictRaw(features))
        outputData = outputData.withColum(predictRawUDF.apply(stage.getRawPredictionCol, SCol(stage.getFeaturesCol)))
        numColsOutput += 1
      }
      if (stage.getPredictionCol != "") {
        val predUDF = if (stage.getRawPredictionCol != "") {
          UDF.make[Double, Vector](features => raw2prediction(features))
            .apply(stage.getPredictionCol, SCol(stage.getRawPredictionCol))
        } else {
          UDF.make[Double, Vector](features => predict(features.asInstanceOf[Vector]))
            .apply(stage.getPredictionCol,SCol(stage.getFeaturesCol))
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

    def predictRaw(features: Vector): Vector

    def raw2prediction(rawPrediction: Vector): Double = rawPrediction.argmax

    def numClasses: Int

  }
