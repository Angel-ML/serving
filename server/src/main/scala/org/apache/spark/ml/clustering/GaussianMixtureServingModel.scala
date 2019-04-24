package org.apache.spark.ml.clustering

import org.apache.spark.ml.data.{SCol, SDFrame, SRow, UDF}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.stat.distribution.MultivariateGaussian
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types._

class GaussianMixtureServingModel(stage: GaussianMixtureModel)
  extends ServingModel[GaussianMixtureServingModel] with GaussianMixtureParams {

  override def copy(extra: ParamMap): GaussianMixtureServingModel = {
    new GaussianMixtureServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)
    val predUDF = UDF.make[Int, Vector](predict, false)
    val probUDF = UDF.make[Vector, Vector](predictProbability, false)
    dataset.withColum(predUDF.apply(stage.getPredictionCol, SCol(stage.getFeaturesCol)))
      .withColum(probUDF.apply(stage.getProbabilityCol, SCol(stage.getFeaturesCol)))
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
    SchemaUtils.checkColumnType(schema, stage.getFeaturesCol, new VectorUDT)
    val schemaWithPredictionCol = SchemaUtils.appendColumn(schema, stage.getPredictionCol, IntegerType)
    SchemaUtils.appendColumn(schemaWithPredictionCol, stage.getProbabilityCol, new VectorUDT)
  }

  override val uid: String = stage.uid

  private def predict(features: Vector): Int = {
    val r = predictProbability(features)
    r.argmax
  }

  private def predictProbability(features: Vector): Vector = {
    val probs: Array[Double] =
      GaussianMixtureServingModel.computeProbabilities(features.asBreeze.toDenseVector, stage.gaussians, stage.weights)
    Vectors.dense(probs)
  }

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.featuresCol)) {
      val schema = new StructType().add(new StructField(stage.getFeaturesCol, new VectorUDT, true))
      new SDFrame(rows)(schema)
    } else {
      throw new Exception (s"featuresCol of ${stage} is not defined!")
    }
  }
}

object GaussianMixtureServingModel {

  def apply(stage: GaussianMixtureModel): GaussianMixtureServingModel = new GaussianMixtureServingModel(stage)

  private def computeProbabilities(
                            features: BDV[Double],
                            dists: Array[MultivariateGaussian],
                            weights: Array[Double]): Array[Double] = {
    val p = weights.zip(dists).map {
      case (weight, dist) => EPSILON + weight * dist.pdf(features)
    }
    val pSum = p.sum
    var i = 0
    while (i < weights.length) {
      p(i) /= pSum
      i += 1
    }
    p
  }

  lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }
}