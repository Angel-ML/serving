package com.tencent.angel.serving.servables.angel

import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef
import com.tencent.angel.serving.apis.prediction.InferenceProtos.{MultiInferenceRequest, MultiInferenceResponse}
import io.grpc.stub.StreamObserver

class MultiInference {

}

object MultiInference {
  def runMultiInference(runOptions: RunOptions, metaGraphDef: MetaGraphDef, servableVersion: Long,
                        session: Session, request: MultiInferenceRequest, responseObserver: StreamObserver[MultiInferenceResponse]): Unit = {
    //todo

  }
}
