package com.tencent.angel.serving.servables.common

import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef
import com.tencent.angel.serving.apis.prediction.RegressionProtos.{RegressionRequest, RegressionResponse}
import com.tencent.angel.serving.servables.angel.{RunOptions, Session}
import org.slf4j.{Logger, LoggerFactory}

object Regressor {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  def runRegress(runOptions: RunOptions, metaGraphDef: MetaGraphDef, servableVersion: Long,
                 session: Session, request: RegressionRequest, responseBuilder: RegressionResponse.Builder): Unit = {
    //todo
  }
}
