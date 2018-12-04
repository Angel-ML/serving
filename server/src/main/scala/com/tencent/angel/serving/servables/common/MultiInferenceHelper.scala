package com.tencent.angel.serving.servables.common

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.InferenceProtos.{MultiInferenceRequest, MultiInferenceResponse}
import com.tencent.angel.serving.core.{ServableHandle, ServableRequest, ServerCore}
import com.tencent.angel.serving.servables.angel.{RunOptions, SavedModelBundle}
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

object MultiInferenceHelper {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  def getModelSpecFromRequest(request: MultiInferenceRequest): ModelSpec = {
    if (request.getTasksCount > 0 && request.getTasks(0).hasModelSpec) {
      return request.getTasks(0).getModelSpec
    }
    ModelSpec.getDefaultInstance
  }

  def runMultiInferenceWithServerCore(runOptions: RunOptions, core: ServerCore, request: MultiInferenceRequest,
                                      responseObserver: StreamObserver[MultiInferenceResponse]): Unit = {
    runMultiInferenceWithServerCoreWithModelSpec(runOptions, core, getModelSpecFromRequest(request),
      request, responseObserver)
  }

  def runMultiInferenceWithServerCoreWithModelSpec(runOptions: RunOptions, core: ServerCore, modelSpec: ModelSpec,
                                                   request: MultiInferenceRequest, responseObserver: StreamObserver[MultiInferenceResponse]): Unit = {
    val servableHandle: ServableHandle[SavedModelBundle] = core.servableHandle(ServableRequest.specific(modelSpec.getName, modelSpec.getVersion.getValue))
    MultiInference.runMultiInference(runOptions, servableHandle.servable.metaGraphDef, servableHandle.id.version,
      servableHandle.servable.session, request, responseObserver)
  }
}
