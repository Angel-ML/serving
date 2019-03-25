package org.apache.spark.ml.feature

import org.apache.spark.ml.feature.{MinHashLSH, MinHashLSHModel}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap

class MinHashLSHServingModel(stage: MinHashLSHModel) extends LSHServingModel[MinHashLSHServingModel] {

  override val hashFunction: Vector => Array[Vector] = {
    elems: Vector => {
      require(elems.numNonzeros > 0, "Must have at least 1 non zero entry.")
      val elemsList = elems.toSparse.indices.toList
      val hashValues = stage.randCoefficients.map { case (a, b) =>
        elemsList.map { elem: Int =>
          ((1 + elem) * a + b) % MinHashLSH.HASH_PRIME
        }.min.toDouble
      }
      // TODO: Output vectors of dimension numHashFunctions in SPARK-18450
      hashValues.map(Vectors.dense(_))
    }
  }

  override def copy(extra: ParamMap): MinHashLSHServingModel = {
    new MinHashLSHServingModel(stage.copy(extra))
  }

  override val uid: String = stage.uid
}

object MinHashLSHServingModel {
  def apply(stage: MinHashLSHModel): MinHashLSHServingModel = new MinHashLSHServingModel(stage)
}
