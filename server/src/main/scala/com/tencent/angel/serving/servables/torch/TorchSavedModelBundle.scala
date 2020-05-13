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
package com.tencent.angel.serving.servables.torch

import java.util

import com.tencent.angel.config.{Entry, Resource, ResourceAllocation}
import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef
import com.tencent.angel.ml.math2.matrix.CooLongFloatMatrix
import com.tencent.angel.pytorch.data.SampleParser
import org.slf4j.{Logger, LoggerFactory}
import com.tencent.angel.pytorch.torch.TorchModel
import com.tencent.angel.serving.apis.common.TypesProtos
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse
import com.tencent.angel.serving.apis.prediction.RequestProtos.Request
import com.tencent.angel.serving.apis.prediction.ResponseProtos.Response
import com.tencent.angel.serving.core.StoragePath
import com.tencent.angel.serving.servables.common.{RunOptions, SavedModelBundle, Session}
import com.tencent.angel.serving.sources.SystemFileUtils
import com.tencent.angel.utils.{InstanceUtils, ProtoUtils}
import org.apache.hadoop.fs.Path
import org.ehcache.sizeof.SizeOf

import scala.collection.mutable.ArrayBuffer

class TorchSavedModelBundle(model: TorchModel) extends SavedModelBundle{
  override val session: Session = null
  override val metaGraphDef: MetaGraphDef = null

  override def unLoad(): Unit = {
    TorchSavedModelBundle.unLoad()
  }

  override def runClassify(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = ???

  override def runMultiInference(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = ???

  override def runRegress(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = ???

  override def runPredict(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = {
    val modelSpec = request.getModelSpec
    responseBuilder.setModelSpec(modelSpec)
    val esb = new StringBuilder

    val numInst = request.getInstancesCount
    if (numInst == 1) {
      try {
        val instance = request.getInstances(0)

        val str = InstanceUtils.getStr(instance)
        val arrayStr =ArrayBuffer[String]()
        arrayStr.append(str)
        val tuple3 =  SampleParser.parse(arrayStr.toArray, model.getType)
        val (coo, fields, targets) = (tuple3._1, tuple3._2, tuple3._3)
        val predictResult = model.forward(1, coo)
        val tempMap = new util.HashMap[String, Float]()
        tempMap.put("pred", predictResult.apply(0))
        responseBuilder.addPredictions(ProtoUtils.getInstance(instance.getName, tempMap))

      } catch {
        case e: Exception =>
          e.printStackTrace()
          esb.append(e.getMessage).append("\n")
        case ae: AssertionError =>
          ae.printStackTrace()
          esb.append(ae.getMessage).append("\n")
        case err: Error =>
          err.printStackTrace()
      }
    } else {
      try {
        val arrayStr =ArrayBuffer[String]()
        (0 until numInst).foreach { idx =>
          val instance = request.getInstances(idx)
          val str = InstanceUtils.getStr(instance)
          arrayStr.append(str)
        }
        val tuple3 =  SampleParser.parse(arrayStr.toArray, model.getType)
        val (coo, fields, targets) = (tuple3._1, tuple3._2, tuple3._3)
        val predictResults = model.forward(numInst, coo)
        predictResults.zipWithIndex.foreach { case (predictResult, idx) =>
          try {
            val instance = request.getInstances(idx)
            val tempMap = new util.HashMap[String, Float]()
            tempMap.put("pred", predictResult)
            responseBuilder.addPredictions(ProtoUtils.getInstance(instance.getName, tempMap))
          } catch {
            case e: Exception =>
              e.printStackTrace()
              esb.append(e.getMessage).append("\n")
          }
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
          esb.append(e.getMessage).append("\n")
      }
    }
  }

  override def fillInputInfo(responseBuilder: GetModelStatusResponse.Builder): Unit = {

  }

  override def getInputInfo(): (TypesProtos.DataType, TypesProtos.DataType, Long) = {
    (TypesProtos.DataType.DT_INVALID, TypesProtos.DataType.DT_INVALID, 148)
  }

}

object TorchSavedModelBundle {
  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private var model: TorchModel = _

  def create(path: StoragePath): SavedModelBundle = {
    // load
    try {
      val fs = SystemFileUtils.getFileSystem(path)
      val fileStatus  = fs.listStatus(new Path(path))
      val filterFileStatus = fileStatus.filter(x => x.isFile)
        .filter(x => x.getPath.toString.endsWith(".pt"))
      val moduleName = filterFileStatus(0).getPath.getName
      val modulePath = path + "/" + moduleName
      LOG.info(s"Begin to load model, load path is " + modulePath)
      TorchModel.setPath(modulePath)
      val model = TorchModel.get()
      LOG.info(s"model has loaded!")
      new TorchSavedModelBundle(model)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        System.exit(-1)
        null.asInstanceOf[TorchSavedModelBundle]
    }
  }

  def resourceEstimate(modelPath: String): ResourceAllocation = {
    if (modelPath != null) {
      if (SystemFileUtils.fileExist(modelPath)) {
        val fileSize = (SystemFileUtils.getTotalSpace(modelPath) * 1.2).toLong
        ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), fileSize)))
      } else {
        ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), 0L)))
      }
    } else {
      ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), 0L)))
    }
  }

  def estimateResourceRequirement(modelPath: String): ResourceAllocation = {
    if (model != null) {
      val sizeOf = SizeOf.newInstance()
      val size = sizeOf.deepSizeOf(model)
      ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), size)))
    } else {
      ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), 0L)))
    }
  }

  def unLoad(): Unit = {
    model = null
  }
}
