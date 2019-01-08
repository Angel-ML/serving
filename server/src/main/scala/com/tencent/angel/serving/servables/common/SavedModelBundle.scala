package com.tencent.angel.serving.servables.common

import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef
import com.tencent.angel.serving.apis.prediction.ClassificationProtos.{ClassificationRequest, ClassificationResponse}
import com.tencent.angel.serving.apis.prediction.InferenceProtos.{MultiInferenceRequest, MultiInferenceResponse}
import com.tencent.angel.serving.apis.prediction.PredictProtos.{PredictRequest, PredictResponse}
import com.tencent.angel.serving.apis.prediction.RegressionProtos.{RegressionRequest, RegressionResponse}
import com.tencent.angel.serving.servables.angel.{RunOptions, Session}

trait SavedModelBundle {
  val session: Session
  val metaGraphDef: MetaGraphDef

  def runClassify(runOptions: RunOptions, request: ClassificationRequest,
                  responseBuilder: ClassificationResponse.Builder): Unit

  def runMultiInference(runOptions: RunOptions, request: MultiInferenceRequest,
                        responseBuilder: MultiInferenceResponse.Builder): Unit

  def runPredict(runOptions: RunOptions, request: PredictRequest,
                 responseBuilder: PredictResponse.Builder): Unit

  def runRegress(runOptions: RunOptions, request: RegressionRequest,
                 responseBuilder: RegressionResponse.Builder): Unit

  def unLoad(): Unit
}
