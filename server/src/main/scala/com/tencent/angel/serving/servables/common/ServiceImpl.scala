package com.tencent.angel.serving.servables.common

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.ClassificationProtos.{ClassificationRequest, ClassificationResponse}
import com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.{GetModelMetadataRequest, GetModelMetadataResponse}
import com.tencent.angel.serving.apis.prediction.InferenceProtos.{MultiInferenceRequest, MultiInferenceResponse}
import com.tencent.angel.serving.apis.prediction.PredictProtos.{PredictRequest, PredictResponse}
import com.tencent.angel.serving.apis.prediction.RegressionProtos.{RegressionRequest, RegressionResponse}
import com.tencent.angel.serving.core.{ServableHandle, ServableRequest, ServerCore}
import com.tencent.angel.serving.servables.angel.RunOptions
import org.slf4j.{Logger, LoggerFactory}

object ServiceImpl {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)

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
    if (!request.hasModelSpec) {
      LOG.info("Missing ModelSpec")
      return
    }

    val servableHandle = getServableHandle(request, core)
    LOG.info(s"servableHandle ${servableHandle.id.toString}")
    servableHandle.servable.runPredict(runOptions, request, responseBuilder)
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
    val servableRequest = ServableRequest.specific(modelSpec.getName, modelSpec.getVersion.getValue)

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
