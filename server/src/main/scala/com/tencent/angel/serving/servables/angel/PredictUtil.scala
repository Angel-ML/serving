package com.tencent.angel.serving.servables.angel

import com.google.protobuf.Int64Value
import com.tencent.angel.core.saver.MetaGraphProtos.{MetaGraphDef, SignatureDef}
import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.PredictProtos.{PredictRequest, PredictResponse}
import io.grpc.stub.StreamObserver

class PredictUtil {

}

object PredictUtil{

  def runPredict(runOptions: RunOptions, metaGraphDef: MetaGraphDef, servableVersion: Long, session: Session,
                 request: PredictRequest, responseObserver: StreamObserver[PredictResponse]): Unit ={
    //todo
    val signatureName: String = if (request.getModelSpec.getSignatureName.isEmpty) "defaultServing" else request.getModelSpec.getSignatureName
    val signature: SignatureDef  = metaGraphDef.getSignatureDefMap.get(signatureName)
    val modelSpec: ModelSpec = ModelSpec.newBuilder().clear().setName(request.getModelSpec.getName)
      .setSignatureName(signatureName).setVersion(Int64Value.newBuilder().setValue(servableVersion).build()).build()
    val builder = PredictResponse.newBuilder().setModelSpec(modelSpec)
    //val outputTensorNames = new ListBuffer[String]
    //val outputTensors = new ListBuffer[TensorProto]
    for((inputName, tensor) <- request.getInputsMap) {
      //outputTensorNames.append(inputName + "Result")
      //outputTensors.append(tensor)
      builder.putOutputs(inputName + "Result", tensor)
    }
    val response = builder.build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }
}
