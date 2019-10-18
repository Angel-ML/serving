/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.serving.service.common

import com.tencent.angel.config.ModelServerConfigProtos.ModelServerConfig
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.{GetModelStatusRequest, GetModelStatusResponse, ModelVersionStatus}
import com.tencent.angel.serving.apis.modelmgr.ModelManagement.{ReloadConfigRequest, ReloadConfigResponse}
import com.tencent.angel.serving.apis.modelmgr.StatusProtos.StatusProto
import com.tencent.angel.serving.apis.modelmgr.{ErrorCodesProtos, ModelServiceGrpc}
import com.tencent.angel.serving.core.ServerCore
import com.tencent.angel.serving.servables.common.SavedModelBundle
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

class ModelServiceImpl extends ModelServiceGrpc.ModelServiceImplBase {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[ModelServiceImpl])
  private var serverCore: ServerCore = _

  def this(serverCore: ServerCore) {
    this()
    this.serverCore = serverCore
  }

  override def getModelStatus(request: GetModelStatusRequest, responseObserver: StreamObserver[GetModelStatusResponse]): Unit = {
    val responseBuilder = GetModelStatusResponse.newBuilder()
    GetModelStatusImpl.getModelStatus(serverCore, request, responseBuilder)

    (0 until responseBuilder.getModelVersionStatusCount).collectFirst{
      case idx if responseBuilder.getModelVersionStatus(idx).getState == ModelVersionStatus.State.AVAILABLE =>
        val servableRequest = serverCore.servableRequestFromModelSpec(request.getModelSpec)
        val servableHandle = serverCore.servableHandle[SavedModelBundle](servableRequest)
        servableHandle.servable.fillInputInfo(responseBuilder)
    }

    responseObserver.onNext(responseBuilder.build())
    responseObserver.onCompleted()
  }

  override def handleReloadConfigRequest(request: ReloadConfigRequest, responseObserver: StreamObserver[ReloadConfigResponse]): Unit = {
    val serverConfig = request.getConfig
    serverConfig.getConfigCase match {
      case ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST =>
        val list = serverConfig.getModelConfigList
        for(index <- 0 until list.getConfigCount) {
          val config = list.getConfig(index)
          LOG.info("Config entry index: " + index + " path: "
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
