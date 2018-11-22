package com.tencent.angel.serving.servables.angel

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.RegressionProtos.{RegressionRequest, RegressionResponse}
import com.tencent.angel.serving.core.{ServableHandle, ServableRequest, ServerCore}
import io.grpc.stub.StreamObserver

object AngelRegressionServiceImpl {
  def regress(runOptions: RunOptions, core: ServerCore,
              request: RegressionRequest, responseObserver: StreamObserver[RegressionResponse]): Unit ={
    if(!request.hasModelSpec) {
      System.err.print("Missing ModelSpec")
      return
    }
    regressWithModelSpec(runOptions, core, request.getModelSpec, request, responseObserver)
  }

  def regressWithModelSpec(runOptions: RunOptions, core: ServerCore, modelSpec: ModelSpec,
                           request: RegressionRequest, responseObserver: StreamObserver[RegressionResponse]): Unit = {
    val servableHandle: ServableHandle[SavedModelBundle] = core.servableHandle(ServableRequest.specific(modelSpec.getName, modelSpec.getVersion.getValue))
    Regressor.runRegress(runOptions, servableHandle.servable.metaGraphDef, servableHandle.id.version,
      servableHandle.servable.session, request, responseObserver)
  }
}
