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
package com.tencent.angel.serving.servables.common

import java.util.concurrent.atomic.AtomicLong

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.{GetModelMetadataRequest, GetModelMetadataResponse}
import com.tencent.angel.serving.apis.prediction.RequestProtos.Request
import com.tencent.angel.serving.apis.prediction.ResponseProtos.Response
import com.tencent.angel.serving.core.{ManagerState, ServableHandle, ServerCore}

import org.slf4j.{Logger, LoggerFactory}

object ServiceImpl {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private val predictionCount = new AtomicLong(1)

  def classify(runOptions: RunOptions, core: ServerCore,
               request: Request, responseBuilder: Response.Builder): Unit = {
    if (!request.hasModelSpec) {
      LOG.info("Missing ModelSpec")
      return
    }

    val servableHandle = getServableHandle(request, core)
    servableHandle.servable.runClassify(runOptions, request, responseBuilder)
  }

  def predict(runOptions: RunOptions, core: ServerCore,
              request: Request, responseBuilder: Response.Builder): Unit = {
    val predictStartTime = System.currentTimeMillis()
    if (!request.hasModelSpec) {
      LOG.info("Missing ModelSpec")
      return
    }

    val servableHandle = getServableHandle(request, core)
    LOG.info(s"servableHandle ${servableHandle.id.toString}")
    var resultStatus = "ok"
    try {
      servableHandle.servable.runPredict(runOptions, request, responseBuilder)
      if(!responseBuilder.getError.isEmpty) {
        resultStatus = "error"
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        resultStatus = "error"
      case _ =>
        resultStatus = "error"
    } finally {
      val predictEndTime = System.currentTimeMillis()
      var elapsedTime: Long = 0
      if(predictEndTime > predictStartTime) {
        elapsedTime = predictEndTime - predictStartTime
      }
      core.createMetricEvent("PredictMetric", predictionCount.getAndIncrement(),
        ManagerState.kEnd, elapsedTime, resultStatus, request.getModelSpec)
    }
  }

  def regress(runOptions: RunOptions, core: ServerCore,
              request: Request, responseBuilder: Response.Builder): Unit = {
    if (!request.hasModelSpec) {
      LOG.info("Missing ModelSpec")
      return
    }

    val servableHandle = getServableHandle(request, core)
    servableHandle.servable.runRegress(runOptions, request, responseBuilder)
  }

  def multiInference(runOptions: RunOptions, core: ServerCore, request: Request,
                     responseBuilder: Response.Builder): Unit = {
    val servableHandle = getServableHandle(request, core)
    servableHandle.servable.runMultiInference(runOptions, request, responseBuilder)
  }

  def modelMetaData(core: ServerCore, request: GetModelMetadataRequest,
                    responseBuilder: GetModelMetadataResponse.Builder): Unit = {
    if (!request.hasModelSpec) {
      LOG.info("Missing ModelSpec")
      return
    }

    // getModelMetadataWithModelSpec(core, request.getModelSpec, request, responseBuilder)
  }


  private def getServableHandle[T](request: T, core: ServerCore): ServableHandle[SavedModelBundle] = {
    val modelSpec = getModelSpecFromRequest[T](request)
    LOG.info(s"modelSpec: ${modelSpec.getName}, ${modelSpec.getVersion}")
    val servableRequest = core.servableRequestFromModelSpec(modelSpec)
    core.servableHandle(servableRequest)
  }

  private def getModelSpecFromRequest[T](request: T): ModelSpec = {
    request match {
      case req: Request => req.getModelSpec
    }
  }
}
