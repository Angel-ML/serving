package com.tencent.angel.serving.servables.angel

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.{GetModelMetadataRequest, GetModelMetadataResponse}
import com.tencent.angel.serving.core.ServerCore
import io.grpc.stub.StreamObserver

object GetModelMetadataImpl {
  def getModelMetaData(core: ServerCore, request: GetModelMetadataRequest,
                       responseObserver: StreamObserver[GetModelMetadataResponse]): Unit = {
    if(!request.hasModelSpec) {
      System.err.print("Missing ModelSpec")
      return
    }
    getModelMetadataWithModelSpec(core, request.getModelSpec, request, responseObserver)
  }

  def getModelMetadataWithModelSpec(core: ServerCore, modelSpec: ModelSpec, request: GetModelMetadataRequest,
                                    responseObserver: StreamObserver[GetModelMetadataResponse]): Unit = {
    //todo
  }

}
