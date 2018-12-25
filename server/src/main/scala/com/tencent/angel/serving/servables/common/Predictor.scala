package com.tencent.angel.serving.servables.common

import java.util.concurrent.atomic.AtomicLong

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.PredictProtos.{PredictRequest, PredictResponse}
import com.tencent.angel.serving.core.{ManagerState, ServableHandle, ServableRequest, ServerCore}
import com.tencent.angel.serving.servables.angel.{RunOptions, SavedModelBundle}
import org.slf4j.{Logger, LoggerFactory}


object Predictor {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private val predictionCount = new AtomicLong(0)

  def predict(runOptions: RunOptions, core: ServerCore,
              request: PredictRequest, responseBuilder: PredictResponse.Builder): Unit ={
    val predictStartTime = System.currentTimeMillis
    if(!request.hasModelSpec) {
      LOG.info("Missing ModelSpec")
      return
    }
    predictWithModelSpec(runOptions, core, request.getModelSpec, request, responseBuilder)
    val predictEndTime = System.currentTimeMillis
    var elapsedTime: Long = 0
    if(predictEndTime > predictStartTime) {
      elapsedTime = predictEndTime - predictStartTime
    }
    core.createMetricEvent("PredictMetric", predictionCount.getAndIncrement(),
      ManagerState.kEnd, elapsedTime, "ok", request.getModelSpec)
  }

  def predictWithModelSpec(runOptions: RunOptions, core: ServerCore, modelSpec: ModelSpec,
                           request: PredictRequest, responseBuilder: PredictResponse.Builder): Unit ={
    val servableHandle: ServableHandle[SavedModelBundle] = core.servableHandle(ServableRequest.specific(modelSpec.getName, modelSpec.getVersion.getValue))
    PredictUtil.runPredict(runOptions, servableHandle.servable.metaGraphDef, servableHandle.id.version,
      servableHandle.servable.session, request, responseBuilder)

  }
}
