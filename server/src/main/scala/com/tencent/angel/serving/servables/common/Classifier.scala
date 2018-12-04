package com.tencent.angel.serving.servables.common

import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef
import com.tencent.angel.serving.apis.prediction.ClassificationProtos.{ClassificationRequest, ClassificationResponse}
import com.tencent.angel.serving.servables.angel.{RunOptions, Session}
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

object Classifier {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  def runClassify(runOptions: RunOptions, metaGraphDef: MetaGraphDef, servableVersion: Long,
                  session: Session, request: ClassificationRequest, responseObserver: StreamObserver[ClassificationResponse]): Unit ={
    //todo
  }
}
