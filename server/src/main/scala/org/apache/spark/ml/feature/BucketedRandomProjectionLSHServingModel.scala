package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap

class BucketedRandomProjectionLSHServingModel(stage: BucketedRandomProjectionLSHModel)
  extends LSHServingModel[BucketedRandomProjectionLSHServingModel] {

  override val hashFunction: Vector => Array[Vector] = {
    key: Vector => {
      val hashValues: Array[Double] = stage.randUnitVectors.map({
        randUnitVector => Math.floor(BLAS.dot(key, randUnitVector) / $(stage.bucketLength))
      })
      // TODO: Output vectors of dimension numHashFunctions in SPARK-18450
      hashValues.map(Vectors.dense(_))
    }
  }

  override def copy(extra: ParamMap): BucketedRandomProjectionLSHServingModel = {
    new BucketedRandomProjectionLSHServingModel(stage.copy(extra))
  }

  override val uid: String = stage.uid
}

object BucketedRandomProjectionLSHServingModel {
  def apply(stage: BucketedRandomProjectionLSHModel): BucketedRandomProjectionLSHServingModel =
    new BucketedRandomProjectionLSHServingModel(stage)
}
