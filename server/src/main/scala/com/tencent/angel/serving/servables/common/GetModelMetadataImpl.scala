package com.tencent.angel.serving.servables.common

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.{GetModelMetadataRequest, GetModelMetadataResponse}
import com.tencent.angel.serving.core.ServerCore
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

object GetModelMetadataImpl {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  def getModelMetaData(core: ServerCore, request: GetModelMetadataRequest,
                       responseObserver: StreamObserver[GetModelMetadataResponse]): Unit = {
    if(!request.hasModelSpec) {
      LOG.info("Missing ModelSpec")
      return
    }
    getModelMetadataWithModelSpec(core, request.getModelSpec, request, responseObserver)
  }

  def getModelMetadataWithModelSpec(core: ServerCore, modelSpec: ModelSpec, request: GetModelMetadataRequest,
                                    responseObserver: StreamObserver[GetModelMetadataResponse]): Unit = {
    //todo
  }

}
