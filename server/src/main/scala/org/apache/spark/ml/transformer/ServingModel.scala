package org.apache.spark.ml.transformer

import org.apache.spark.ml.param.ParamMap

abstract class ServingModel[M <: ServingModel[M]] extends ServingTrans {

  override def copy(extra: ParamMap): M
}
