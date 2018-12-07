package com.tencent.angel.serving.service.common

import com.tencent.angel.config.ModelServerConfigProtos.ModelServerConfig
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.{GetModelStatusRequest, GetModelStatusResponse}
import com.tencent.angel.serving.apis.modelmgr.ModelManagement.{ReloadConfigRequest, ReloadConfigResponse}
import com.tencent.angel.serving.apis.modelmgr.StatusProtos.StatusProto
import com.tencent.angel.serving.apis.modelmgr.{ErrorCodesProtos, ModelServiceGrpc}
import com.tencent.angel.serving.core.ServerCore
import io.grpc.stub.StreamObserver

class ModelServiceImpl extends ModelServiceGrpc.ModelServiceImplBase {

  private var serverCore: ServerCore = _

  def this(serverCore: ServerCore) {
    this()
    this.serverCore = serverCore
  }

  override def getModelStatus(request: GetModelStatusRequest, responseObserver: StreamObserver[GetModelStatusResponse]): Unit = {
    val responseBuilder = GetModelStatusResponse.newBuilder()
    GetModelStatusImpl.getModelStatus(serverCore, request, responseBuilder)
    val getModelStatusResponse = responseBuilder.build()
    responseObserver.onNext(getModelStatusResponse)
    responseObserver.onCompleted()
  }

  override def handleReloadConfigRequest(request: ReloadConfigRequest, responseObserver: StreamObserver[ReloadConfigResponse]): Unit = {
    val serverConfig = request.getConfig
    serverConfig.getConfigCase match {
      case ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST =>
        val list = serverConfig.getModelConfigList
        for(index <- 0 until list.getConfigCount) {
          val config = list.getConfig(index)
          System.out.print("Config entry index: " + index + " path: "
            + config.getBasePath + " name: " + config.getName + "platform: " + config.getModelPlatform)
        }
        serverCore.reloadConfig(serverConfig)
    }
    val reloadConfigResponse = ReloadConfigResponse.newBuilder()
      .setStatus(StatusProto.newBuilder().setErrorCode(ErrorCodesProtos.Code.OK)).build()
    responseObserver.onNext(reloadConfigResponse)
    responseObserver.onCompleted()
  }
}
