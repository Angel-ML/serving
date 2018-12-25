package com.tencent.angel.serving.servables

import com.tencent.angel.core.graph.TensorProtos.TensorProto
import com.tencent.angel.core.graph.TypesProtos
import com.tencent.angel.ml.core.PredictResult
import com.tencent.angel.utils.ProtoUtils


object Utils {

  def predictResult2TensorProto(result: PredictResult): TensorProto = {
    val builder = TensorProto.newBuilder()

    builder.setTensorShape(ProtoUtils.toShape(2L))
    builder.setDtype(TypesProtos.DataType.DT_DOUBLE)

    builder.addDoubleVal(result.predLabel)
    builder.addDoubleVal(result.pred)

    builder.build()
  }
}
