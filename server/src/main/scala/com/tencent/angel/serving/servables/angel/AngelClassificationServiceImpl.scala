package com.tencent.angel.serving.servables.angel

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.ClassificationProtos.{ClassificationRequest, ClassificationResponse}
import com.tencent.angel.serving.core.{ServableHandle, ServableRequest, ServerCore}
import io.grpc.stub.StreamObserver

object AngelClassificationServiceImpl {

  def classify(runOptions: RunOptions, core: ServerCore,
               request: ClassificationRequest, responseObserver: StreamObserver[ClassificationResponse]): Unit = {
    if(!request.hasModelSpec){
      System.err.print("Missing ModelSpec")
      return
    }
    classifyWithModelSpec(runOptions, core, request.getModelSpec, request, responseObserver)
  }

  def classifyWithModelSpec(runOptions: RunOptions, core: ServerCore, modelSpec: ModelSpec,
                            request: ClassificationRequest, responseObserver: StreamObserver[ClassificationResponse]): Unit = {
    val servableHandle: ServableHandle[SavedModelBundle] = core.servableHandle(ServableRequest.specific(modelSpec.getName, modelSpec.getVersion.getValue))
    Classifier.runClassify(runOptions, servableHandle.servable.metaGraphDef, servableHandle.id.version,
      servableHandle.servable.session, request, responseObserver)
  }

}