package com.tencent.angel.serving.servables.common

import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef
import com.tencent.angel.serving.apis.prediction.InferenceProtos.{MultiInferenceRequest, MultiInferenceResponse}
import com.tencent.angel.serving.servables.angel.{RunOptions, Session}
import org.slf4j.{Logger, LoggerFactory}

object MultiInference {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  def runMultiInference(runOptions: RunOptions, metaGraphDef: MetaGraphDef, servableVersion: Long,
                        session: Session, request: MultiInferenceRequest, responseBuilder: MultiInferenceResponse.Builder): Unit = {
    //todo

  }
}
