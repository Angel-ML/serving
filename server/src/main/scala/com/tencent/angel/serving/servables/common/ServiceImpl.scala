package com.tencent.angel.serving.servables.common

import java.util.concurrent.atomic.AtomicLong

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.ClassificationProtos.{ClassificationRequest, ClassificationResponse}
import com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.{GetModelMetadataRequest, GetModelMetadataResponse}
import com.tencent.angel.serving.apis.prediction.InferenceProtos.{MultiInferenceRequest, MultiInferenceResponse}
import com.tencent.angel.serving.apis.prediction.PredictProtos.{PredictRequest, PredictResponse}
import com.tencent.angel.serving.apis.prediction.RegressionProtos.{RegressionRequest, RegressionResponse}
import com.tencent.angel.serving.core.{ManagerState, ServableHandle, ServableRequest, ServerCore}
import com.tencent.angel.serving.servables.angel.RunOptions
import org.slf4j.{Logger, LoggerFactory}

object ServiceImpl {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private val predictionCount = new AtomicLong(0)

  def classify(runOptions: RunOptions, core: ServerCore,
               request: ClassificationRequest, responseBuilder: ClassificationResponse.Builder): Unit = {
    if (!request.hasModelSpec) {
      LOG.info("Missing ModelSpec")
      return
    }

    val servableHandle = getServableHandle(request, core)
    servableHandle.servable.runClassify(runOptions, request, responseBuilder)
  }

  def predict(runOptions: RunOptions, core: ServerCore,
              request: PredictRequest, responseBuilder: PredictResponse.Builder): Unit = {
    val predictStartTime = System.currentTimeMillis()
    if (!request.hasModelSpec) {
      LOG.info("Missing ModelSpec")
      return
    }

    val servableHandle = getServableHandle(request, core)
    LOG.info(s"servableHandle ${servableHandle.id.toString}")
    servableHandle.servable.runPredict(runOptions, request, responseBuilder)
    val predictEndTime = System.currentTimeMillis()
    var elapsedTime: Long = 0
    if(predictEndTime > predictStartTime) {
      elapsedTime = predictEndTime - predictStartTime
    }
    core.createMetricEvent("PredictMetric", predictionCount.getAndIncrement(),
      ManagerState.kEnd, elapsedTime, "ok", request.getModelSpec)
  }

  def regress(runOptions: RunOptions, core: ServerCore,
              request: RegressionRequest, responseBuilder: RegressionResponse.Builder): Unit = {
    if (!request.hasModelSpec) {
      LOG.info("Missing ModelSpec")
      return
    }

    val servableHandle = getServableHandle(request, core)
    servableHandle.servable.runRegress(runOptions, request, responseBuilder)
  }

  def multiInference(runOptions: RunOptions, core: ServerCore, request: MultiInferenceRequest,
                     responseBuilder: MultiInferenceResponse.Builder): Unit = {
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
      case req: MultiInferenceRequest =>
        if (req.getTasksCount > 0 && req.getTasks(0).hasModelSpec) {
          req.getTasks(0).getModelSpec
        } else {
          ModelSpec.getDefaultInstance
        }
      case req: ClassificationRequest => req.getModelSpec
      case req: PredictRequest => req.getModelSpec
      case req: RegressionRequest => req.getModelSpec
    }
  }
}
