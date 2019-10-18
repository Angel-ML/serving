/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.serving.servables.common

import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef
import com.tencent.angel.serving.apis.common.TypesProtos
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse
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

  def fillInputInfo(responseBuilder: GetModelStatusResponse.Builder): Unit

  def getInputInfo(): (TypesProtos.DataType, TypesProtos.DataType, Long)
}
