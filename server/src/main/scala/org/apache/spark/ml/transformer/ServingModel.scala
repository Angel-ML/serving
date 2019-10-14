package org.apache.spark.ml.transformer

import org.apache.spark.ml.param.ParamMap

abstract class ServingModel[M <: ServingModel[M]] extends ServingTrans {

  private var value_Type: String = "double"

  override def copy(extra: ParamMap): M

  override def valueType(): String = value_Type

  def setValueType(vtype: String): Unit = {
    value_Type = vtype
  }

}
