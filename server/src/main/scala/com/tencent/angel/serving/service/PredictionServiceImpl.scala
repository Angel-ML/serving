package com.tencent.angel.serving.service

import com.tencent.angel.serving.apis.prediction.ClassificationProtos.{ClassificationRequest, ClassificationResponse}
import com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.{GetModelMetadataRequest, GetModelMetadataResponse}
import com.tencent.angel.serving.apis.prediction.InferenceProtos.{MultiInferenceRequest, MultiInferenceResponse}
import com.tencent.angel.serving.apis.prediction.PredictProtos.{PredictRequest, PredictResponse}
import com.tencent.angel.serving.apis.prediction.RegressionProtos.{RegressionRequest, RegressionResponse}
import com.tencent.angel.serving.apis.prediction._
import com.tencent.angel.serving.core.ServerCore
import com.tencent.angel.serving.servables.angel._
import io.grpc.stub.StreamObserver


class PredictionServiceImpl extends PredictionServiceGrpc.PredictionServiceImplBase {

  private var serverCore: ServerCore = _
  private var predictor: AngelPredictor = _

  def this(serverCore: ServerCore, predictor: AngelPredictor) {
    this()
    this.serverCore = serverCore
    this.predictor = predictor
  }

  override def predict(request: PredictRequest, responseObserver: StreamObserver[PredictResponse]): Unit = {
    val runOptions = new RunOptions()
    predictor.predict(runOptions, serverCore, request, responseObserver)
  }

  override def classify(request: ClassificationRequest, responseObserver: StreamObserver[ClassificationResponse]): Unit = {
    val runOptions = new RunOptions()
    AngelClassificationServiceImpl.classify(runOptions, serverCore, request, responseObserver)
  }

  override def regress(request: RegressionRequest, responseObserver: StreamObserver[RegressionResponse]): Unit = {
    val runOptions = new RunOptions()
    AngelRegressionServiceImpl.regress(runOptions, serverCore, request, responseObserver)
  }

  override def multiInference(request: MultiInferenceRequest, responseObserver: StreamObserver[MultiInferenceResponse]): Unit = {
    val runOptions = new RunOptions()
    MultiInferenceHelper.runMultiInferenceWithServerCore(runOptions, serverCore, request, responseObserver)
  }

  override def getModelMetadata(request: GetModelMetadataRequest, responseObserver: StreamObserver[GetModelMetadataResponse]): Unit = {
    GetModelMetadataImpl.getModelMetaData(serverCore, request, responseObserver)
    System.out.print("getModelMetadata")
  }

}