package com.tencent.angel.serving.servables.angel

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.PredictProtos.{PredictRequest, PredictResponse}
import com.tencent.angel.serving.core.{ServableHandle, ServableRequest, ServerCore}
import io.grpc.stub.StreamObserver


class AngelPredictor {

  def predict(runOptions: RunOptions, core: ServerCore,
              request: PredictRequest, responseObserver: StreamObserver[PredictResponse]): Unit ={
    if(!request.hasModelSpec) {
      System.err.print("Missing ModelSpec")
      return
    }
    predictWithModelSpec(runOptions, core, request.getModelSpec, request, responseObserver)
  }

  def predictWithModelSpec(runOptions: RunOptions, core: ServerCore, modelSpec: ModelSpec,
                           request: PredictRequest, responseObserver: StreamObserver[PredictResponse]): Unit ={
    val servableHandle: ServableHandle[SavedModelBundle] = core.servableHandle(ServableRequest.specific(modelSpec.getName, modelSpec.getVersion.getValue))
    PredictUtil.runPredict(runOptions, servableHandle.servable.metaGraphDef, servableHandle.id.version,
      servableHandle.servable.session, request, responseObserver)
  }
}
