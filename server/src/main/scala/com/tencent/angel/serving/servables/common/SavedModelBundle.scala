package com.tencent.angel.serving.servables.common

import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef
import com.tencent.angel.serving.apis.prediction.RequestProtos.Request
import com.tencent.angel.serving.apis.prediction.ResponseProtos.Response

class RunOptions

class Session

trait SavedModelBundle {
  val session: Session
  val metaGraphDef: MetaGraphDef

  def runClassify(runOptions: RunOptions, request: Request,
                  responseBuilder: Response.Builder): Unit

  def runMultiInference(runOptions: RunOptions, request: Request,
                        responseBuilder: Response.Builder): Unit

  def runPredict(runOptions: RunOptions, request: Request,
                 responseBuilder: Response.Builder): Unit

  def runRegress(runOptions: RunOptions, request: Request,
                 responseBuilder: Response.Builder): Unit

  def unLoad(): Unit
}
