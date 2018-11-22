package com.tencent.angel.serving.servables.angel

import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef
import com.tencent.angel.serving.apis.prediction.ClassificationProtos.{ClassificationRequest, ClassificationResponse}
import io.grpc.stub.StreamObserver

class Classifier {

}

object Classifier {
  def runClassify(runOptions: RunOptions, metaGraphDef: MetaGraphDef, servableVersion: Long,
                  session: Session, request: ClassificationRequest, responseObserver: StreamObserver[ClassificationResponse]): Unit ={
    //todo
  }
}
