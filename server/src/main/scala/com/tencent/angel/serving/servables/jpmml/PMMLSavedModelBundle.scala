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
package com.tencent.angel.serving.servables.jpmml

import java.io.{IOException, InputStream}
import java.util

import com.tencent.angel.config.{Entry, Resource, ResourceAllocation}
import com.tencent.angel.core.saver.MetaGraphProtos._
import com.tencent.angel.utils.{InstanceUtils, ProtoUtils}
import com.tencent.angel.serving.apis.common.InstanceProtos.InstanceFlag
import com.tencent.angel.serving.apis.prediction.RequestProtos.Request
import com.tencent.angel.serving.apis.prediction.ResponseProtos.Response
import com.tencent.angel.serving.core.StoragePath
import com.tencent.angel.serving.servables.common.{RunOptions, SavedModelBundle, Session}
import javax.xml.bind.JAXBException
import org.dmg.pmml.{FieldName, PMML}
import org.jpmml.evaluator._
import org.jpmml.model.PMMLUtil
import org.slf4j.{Logger, LoggerFactory}
import org.xml.sax.SAXException
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.tencent.angel.serving.apis.common.TypesProtos
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse
import com.tencent.angel.serving.sources.SystemFileUtils
import org.apache.hadoop.fs.Path
import org.ehcache.sizeof.SizeOf

import scala.collection.JavaConversions._


import scala.collection.JavaConversions._
import org.dmg.pmml.DataType

class PMMLSavedModelBundle(val pmml: PMML) extends SavedModelBundle {
  override val session: Session = null
  override val metaGraphDef: MetaGraphDef = null

  private val evaluator: Evaluator = ModelEvaluatorFactory.newInstance.newModelEvaluator(pmml)
  evaluator.verify()

  private val inputFields: util.List[InputField] = evaluator.getInputFields
  private val fieldNameSet = new util.HashSet[String]()
  for (fieldName: FieldName <- EvaluatorUtil.getNames(inputFields)) {
    fieldNameSet.add(fieldName.getValue)
  }

  private val groupFields: util.List[InputField] = evaluator match {
    case groupFiled: HasGroupFields => groupFiled.getGroupFields
    case _ => new util.ArrayList[InputField]()
  }
  private val groupFieldNameSet = new util.HashSet[String]()
  for (fieldName: FieldName <- EvaluatorUtil.getNames(groupFields)) {
    groupFieldNameSet.add(fieldName.getValue)
  }

  private val resultFields: util.List[FieldName] = new util.ArrayList[FieldName]()
  private val targetFields: util.List[TargetField] = evaluator.getTargetFields
  for (targetField: TargetField <- targetFields) {
    resultFields.add(targetField.getName)
  }

  private val outputFields: util.List[OutputField] = evaluator.getOutputFields
  for (outputField: OutputField <- outputFields) {
    resultFields.add(outputField.getName)
  }

  override def runClassify(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = {
    runPredict(runOptions, request, responseBuilder)
  }

  override def runMultiInference(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = {
    runPredict(runOptions, request, responseBuilder)
  }

  override def runPredict(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = {
    val numInstances = request.getInstancesCount

    responseBuilder.setModelSpec(request.getModelSpec)
    val esb = new StringBuilder()
    (0 until numInstances).foreach { idx =>
      val instance = request.getInstances(idx)
      assert(instance.getFlag == InstanceFlag.IF_STRINGKEY_VECTOR)

      val dataMap = InstanceUtils.getStringKeyMap(instance)

      val missingInputFields = Sets.difference(fieldNameSet, dataMap.keySet())
      val missingGroupFields = Sets.difference(groupFieldNameSet, dataMap.keySet())

      val arguments = new util.LinkedHashMap[FieldName, FieldValue]()
      val outputRecords: util.Map[String, String] = new util.HashMap[String, String]()
      if (!missingInputFields.isEmpty) {
        esb.append(s"Instance ${instance.getName} Missing input field(s): " + missingInputFields).append("\n")
      } else if (!missingGroupFields.isEmpty) {
        esb.append(s"Instance ${instance.getName} Missing group field(s): " + missingGroupFields).append("\n")
      } else {
        try {
          for (inputField: InputField <- inputFields) {
            val inputFieldName = inputField.getName
            val rawValue = dataMap.get(inputFieldName.getValue)
            val inputFieldValue = inputField.prepare(rawValue)
            arguments.put(inputFieldName, inputFieldValue)
          }

          val newArguments = evaluator match {
            case hasGroupFields: HasGroupFields =>
              EvaluatorUtil.groupRows(hasGroupFields, Lists.newArrayList(arguments)).get(0)
            case _ => arguments
          }

          val results: util.Map[FieldName, _] = evaluator.evaluate(newArguments)
          for (fieldName: FieldName <- resultFields) {
            val name = fieldName.getValue
            val valueObject = EvaluatorUtil.decode(results.get(fieldName))
            val value = if (valueObject == null) {
              "N/A"
            } else {
              valueObject.toString
            }

            outputRecords.put(name, value)
          }

          responseBuilder.addPredictions(ProtoUtils.getInstance(instance.getName, outputRecords))
        } catch {
          case e: Exception =>
            e.printStackTrace()
            esb.append(e.getMessage).append("\n")
          case ae: AssertionError =>
            ae.printStackTrace()
            esb.append(ae.getMessage).append("\n")
        }
      }

      outputRecords.clear()
      arguments.clear()
    }

    if (esb.nonEmpty) {
      responseBuilder.setError(esb.toString())
    }
  }

  override def runRegress(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = {
    runPredict(runOptions, request, responseBuilder)
  }

  override def unLoad(): Unit = {
    PMMLSavedModelBundle.unLoad()
  }

  override def fillInputInfo(responseBuilder: GetModelStatusResponse.Builder): Unit = {
    for (inputField: InputField <- inputFields) {
      val fieldName = inputField.getName.getValue

      inputField.getDataType match {
        case DataType.STRING => responseBuilder.putTypeMap(fieldName, TypesProtos.DataType.DT_STRING)
        case DataType.INTEGER => responseBuilder.putTypeMap(fieldName, TypesProtos.DataType.DT_INT32)
        case DataType.FLOAT => responseBuilder.putTypeMap(fieldName, TypesProtos.DataType.DT_FLOAT)
        case DataType.DOUBLE => responseBuilder.putTypeMap(fieldName, TypesProtos.DataType.DT_DOUBLE)
        case _ => responseBuilder.putTypeMap(fieldName, TypesProtos.DataType.DT_INVALID)
      }
    }

  }

  override def getInputInfo(): (TypesProtos.DataType, TypesProtos.DataType, Long) = {
    var keyType: TypesProtos.DataType = TypesProtos.DataType.DT_INVALID
    var valueType: TypesProtos.DataType = TypesProtos.DataType.DT_INVALID
    var dim: Long = -1
    inputFields.get(0).getDataType match {
      case DataType.STRING => valueType = TypesProtos.DataType.DT_STRING
      case DataType.INTEGER => valueType = TypesProtos.DataType.DT_INT32
      case DataType.FLOAT => valueType = TypesProtos.DataType.DT_FLOAT
      case DataType.DOUBLE => valueType = TypesProtos.DataType.DT_DOUBLE
      case _ => valueType = TypesProtos.DataType.DT_INVALID
    }
    (keyType, valueType, dim)
  }

}


object PMMLSavedModelBundle {
  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private var pmml: PMML = _

  def create(path: StoragePath): PMMLSavedModelBundle = {
    var inputStream: InputStream = null
    try {
      val fs = SystemFileUtils.getFileSystem()
      val fileStatus  = fs.listStatus(new Path(path))
      val filterFileStatus = fileStatus.filter(x => x.isFile)
        .filter(x => x.getPath.toString.endsWith(".pmml") || x.getPath.toString.endsWith(".txt"))
      LOG.info("Begin to load model ..., model path is: " + filterFileStatus(0).getPath)
      inputStream = fs.open(filterFileStatus(0).getPath)
      pmml = PMMLUtil.unmarshal(inputStream)
      LOG.info("End to load model ...")
      new PMMLSavedModelBundle(pmml)
    } catch {
      case e: JAXBException =>
        e.printStackTrace()
        System.exit(-1)
        null.asInstanceOf[PMMLSavedModelBundle]
      case e: SAXException =>
        e.printStackTrace()
        System.exit(-1)
        null.asInstanceOf[PMMLSavedModelBundle]
      case e: IOException =>
        e.printStackTrace()
        System.exit(-1)
        null.asInstanceOf[PMMLSavedModelBundle]
    } finally {
      if (inputStream != null) {
        inputStream.close()
      }
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
    if (pmml != null) {
      val sizeOf =  SizeOf.newInstance()
      val size = sizeOf.deepSizeOf(pmml)
      LOG.info("pmml model size is: " + size)
      ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), size)))
    } else {
      ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), 0L)))
    }
  }

  def unLoad(): Unit = {
    pmml = null
  }
}
