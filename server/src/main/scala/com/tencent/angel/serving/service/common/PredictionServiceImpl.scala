package com.tencent.angel.serving.service.common

import com.tencent.angel.serving.apis.prediction.ClassificationProtos.{ClassificationRequest, ClassificationResponse}
import com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.{GetModelMetadataRequest, GetModelMetadataResponse}
import com.tencent.angel.serving.apis.prediction.InferenceProtos.{MultiInferenceRequest, MultiInferenceResponse}
import com.tencent.angel.serving.apis.prediction.PredictProtos.{PredictRequest, PredictResponse}
import com.tencent.angel.serving.apis.prediction.RegressionProtos.{RegressionRequest, RegressionResponse}
import com.tencent.angel.serving.apis.prediction._
import com.tencent.angel.serving.core.ServerCore
import com.tencent.angel.serving.servables.angel._
import com.tencent.angel.serving.servables.common._
import io.grpc.stub.StreamObserver


class PredictionServiceImpl extends PredictionServiceGrpc.PredictionServiceImplBase {

  private var serverCore: ServerCore = _

  def this(serverCore: ServerCore) {
    this()
    this.serverCore = serverCore
  }

  override def predict(request: PredictRequest, responseObserver: StreamObserver[PredictResponse]): Unit = {
    val runOptions = new RunOptions()
    val responseBuilder = PredictResponse.newBuilder()
    Predictor.predict(runOptions, serverCore, request, responseBuilder)
    val predictResponse = responseBuilder.build()
    responseObserver.onNext(predictResponse)
    responseObserver.onCompleted()
  }

  override def classify(request: ClassificationRequest, responseObserver: StreamObserver[ClassificationResponse]): Unit = {
    val runOptions = new RunOptions()
    val responseBuilder = ClassificationResponse.newBuilder()
    ClassificationServiceImpl.classify(runOptions, serverCore, request, responseBuilder)
    val classificationResponse = responseBuilder.build()
    responseObserver.onNext(classificationResponse)
    responseObserver.onCompleted()
  }

  override def regress(request: RegressionRequest, responseObserver: StreamObserver[RegressionResponse]): Unit = {
    val runOptions = new RunOptions()
    val responseBuilder = RegressionResponse.newBuilder()
    RegressionServiceImpl.regress(runOptions, serverCore, request, responseBuilder)
    val regressionResponse = responseBuilder.build()
    responseObserver.onNext(regressionResponse)
    responseObserver.onCompleted()
  }

  override def multiInference(request: MultiInferenceRequest, responseObserver: StreamObserver[MultiInferenceResponse]): Unit = {
    val runOptions = new RunOptions()
    val responseBuilder = MultiInferenceResponse.newBuilder()
    MultiInferenceHelper.runMultiInferenceWithServerCore(runOptions, serverCore, request, responseBuilder)
    val multiInferenceResponse = responseBuilder.build()
    responseObserver.onNext(multiInferenceResponse)
    responseObserver.onCompleted()
  }

  override def getModelMetadata(request: GetModelMetadataRequest, responseObserver: StreamObserver[GetModelMetadataResponse]): Unit = {
    val responseBuilder = GetModelMetadataResponse.newBuilder()
    GetModelMetadataImpl.getModelMetaData(serverCore, request, responseBuilder)
    val getModelMetadataResponse = responseBuilder.build()
    responseObserver.onNext(getModelMetadataResponse)
    responseObserver.onCompleted()
  }

}