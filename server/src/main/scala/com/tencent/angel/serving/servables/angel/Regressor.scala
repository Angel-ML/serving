package com.tencent.angel.serving.servables.angel

import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef
import com.tencent.angel.serving.apis.prediction.RegressionProtos.{RegressionRequest, RegressionResponse}
import io.grpc.stub.StreamObserver

class Regressor {

}

object Regressor {
  def runRegress(runOptions: RunOptions, metaGraphDef: MetaGraphDef, servableVersion: Long,
                 session: Session, request: RegressionRequest, responseObserver: StreamObserver[RegressionResponse]): Unit = {
    //todo
  }
}
