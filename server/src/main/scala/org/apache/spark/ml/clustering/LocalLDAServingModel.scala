package org.apache.spark.ml.clustering

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.clustering.{LocalLDAModel => OldLocalLDAModel}

class LocalLDAServingModel(stage: LocalLDAModel) extends LDAServingModel[LocalLDAServingModel, LocalLDAModel](stage) {

  override def oldLocalModel: OldLocalLDAModel = stage.oldLocalModel

  override def copy(extra: ParamMap): LocalLDAServingModel = {
    new LocalLDAServingModel(stage.copy(extra))
  }

  override val uid: String = stage.uid
}

object LocalLDAServingModel {
  def apply(stage: LocalLDAModel): LocalLDAServingModel = new LocalLDAServingModel(stage)
}
