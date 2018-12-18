package com.tencent.angel.serving.servables.common

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.{GetModelMetadataRequest, GetModelMetadataResponse}
import com.tencent.angel.serving.core.ServerCore
import org.slf4j.{Logger, LoggerFactory}

object GetModelMetadataImpl {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  def getModelMetaData(core: ServerCore, request: GetModelMetadataRequest,
                       responseBuilder: GetModelMetadataResponse.Builder): Unit = {
    if(!request.hasModelSpec) {
      LOG.info("Missing ModelSpec")
      return
    }
    getModelMetadataWithModelSpec(core, request.getModelSpec, request, responseBuilder)
  }

  def getModelMetadataWithModelSpec(core: ServerCore, modelSpec: ModelSpec, request: GetModelMetadataRequest,
                                    responseBuilder: GetModelMetadataResponse.Builder): Unit = {
    //todo
  }

}