package com.tencent.angel.serving.servables.common

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.RegressionProtos.{RegressionRequest, RegressionResponse}
import com.tencent.angel.serving.core.{ServableHandle, ServableRequest, ServerCore}
import com.tencent.angel.serving.servables.angel.{RunOptions, SavedModelBundle}
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

object RegressionServiceImpl {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  def regress(runOptions: RunOptions, core: ServerCore,
              request: RegressionRequest, responseObserver: StreamObserver[RegressionResponse]): Unit ={
    if(!request.hasModelSpec) {
      LOG.info("Missing ModelSpec")
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
