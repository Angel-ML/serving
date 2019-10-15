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
package com.tencent.angel.serving.service.jersey.resources

import java.util

import com.google.protobuf.Int64Value
import com.google.protobuf.util.JsonFormat
import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.{GetModelStatusRequest, GetModelStatusResponse, ModelVersionStatus}
import com.tencent.angel.serving.apis.prediction.RequestProtos.Request
import com.tencent.angel.serving.apis.prediction.ResponseProtos
import com.tencent.angel.serving.servables.common.{RunOptions, SavedModelBundle, ServiceImpl}
import com.tencent.angel.serving.service.ModelServer
import com.tencent.angel.serving.service.common.GetModelStatusImpl
import com.tencent.angel.serving.service.jersey.util.Json2Instances
import com.tencent.angel.utils.InstanceUtils
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import org.json4s.native.Serialization._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex

@Path("/")
class HttpRestApiHandler {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[ModelServer])

  @GET
  @Path("/v1/models{requestPath:(?:/([^/:]+))?(?:/versions/(\\d+))?}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def processModelServiceRequest(@PathParam("requestPath") requestPath: String): Response = {
    var modelName: String = null
    var modelVersion: String = null
    try{
      val pattern = new Regex("""(?:/([^/:]+))?(?:/versions/(\d+))?""")
      val pattern(_, _) = requestPath
      requestPath match {
        case pattern(name, version) =>
          modelName = name
          modelVersion = version
        case _ =>
          LOG.info("unsupported request url.")
      }
    } catch {
      case ex: Exception =>
        val errorMessage = "Resolve request path error, exception: " + ex
        return Response.status(500).entity("{\"error\": \""+  errorMessage + "\"}").build()
    }
    if(modelName == null || modelName.isEmpty) {
      val errorMessage = "Missing model name in request."
      LOG.info(errorMessage)
      Response.status(500).entity("{\"error\": \""+  errorMessage + "\"}").build()
    } else {
      val modelSpecBuilder = ModelSpec.newBuilder()
      modelSpecBuilder.setName(modelName)
      if(modelVersion != null && !modelVersion.isEmpty) {
        modelSpecBuilder.setVersion(Int64Value.newBuilder().setValue(modelVersion.toLong))
      }
      val modelSpec = modelSpecBuilder.build()
      val request = GetModelStatusRequest.newBuilder().setModelSpec(modelSpec).build()
      val builder = GetModelStatusResponse.newBuilder()
      GetModelStatusImpl.getModelStatus(ModelServer.getServerCore, request, builder)
      (0 until builder.getModelVersionStatusCount).collectFirst{
        case idx if builder.getModelVersionStatus(idx).getState == ModelVersionStatus.State.AVAILABLE =>
          val servableRequest = ModelServer.getServerCore.servableRequestFromModelSpec(request.getModelSpec)
          val servableHandle = ModelServer.getServerCore.servableHandle[SavedModelBundle](servableRequest)
          servableHandle.servable.fillInputInfo(builder)
      }
      val jsonFormat = JsonFormat.printer().print(builder.build())
      Response.status(200).entity(jsonFormat).build()
    }
  }

  @GET
  @Path("/monitoring/prometheus/metrics")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def processMetricsRequest(): Response = {
    val result =  ModelServer.getServerCore.context.metricsManager.getMetricsResult()
    Response.status(200).entity(result).build()
  }

  @POST
  @Path("/v1/models/{requestPath:([^/:]+)(?:/versions/(\\d+))?:(classify|regress|predict)}")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def processPredictionServiceRequest(requestBody: String, @PathParam("requestPath") requestPath: String): Response ={
    var modelName: String = null
    var modelVersion: String = null
    var modelMethod: String = null
    try {
      val pattern = new Regex("""([^/:]+)(?:/versions/(\d+))?:(classify|regress|predict)""")
      val pattern(_, _, _) = requestPath
      requestPath match {
        case pattern(name, version, method) =>
          modelName = name
          modelVersion = version
          modelMethod = method
        case _ =>
          LOG.info("unsupported request url.")
      }
    } catch {
      case ex: Exception =>
        val errorMessage = "Resolve request path error, exception: " + ex
        return Response.status(500).entity("{\"error\": \""+  errorMessage + "\"}").build()
    }
    var output: String = null
    if(modelMethod.equals("classify")) {
      processClassifyRequest()
    } else if(modelMethod.equals("predict")) {
      output = processPredictRequest(modelName, modelVersion, requestBody)
    } else if(modelMethod.equals("regress")) {
      processRegressRequest()
    } else {
      val errorMessage = "model method: " + modelMethod + " can not found."
      LOG.info(errorMessage)
      return Response.status(500).entity("{\"error\": \""+  errorMessage + "\"}").build()
    }
    Response.status(200).entity(output).build()
  }

  def processClassifyRequest(): Unit = {

  }

  def processPredictRequest(modelName: String, modelVersion: String, requestBody: String): String = {
    val modelSpecBuilder = ModelSpec.newBuilder()
    modelSpecBuilder.setName(modelName)
    if(modelVersion !=null && !modelVersion.isEmpty) {
      modelSpecBuilder.setVersion(Int64Value.newBuilder().setValue(modelVersion.toLong))
    }
    val requestBuilder = Request.newBuilder()
    requestBuilder.setModelSpec(modelSpecBuilder.build())
    Json2Instances.fillPredictRequestFromJson(requestBody, requestBuilder)
    val responseBuilder = ResponseProtos.Response.newBuilder()
    val runOptions = new RunOptions()
    ServiceImpl.predict(runOptions, ModelServer.getServerCore, requestBuilder.build(), responseBuilder)
    val response = responseBuilder.build()
    val resultCount = response.getPredictionsCount
    if(resultCount > 0) {
      import scala.collection.mutable
      val results = new util.ArrayList[mutable.Map[String, _<:Any]]()
      val iter = response.getPredictionsList.iterator()
      implicit val formats = org.json4s.DefaultFormats
      while(iter.hasNext) {
        import scala.collection.JavaConverters._
        results.add(InstanceUtils.getStringKeyMap(iter.next()).asScala)
      }
      "{\"predictions\": " + write(results) + "}"
    } else {
      "{\"error\": \""+ response.getError + "\"}"
    }
  }

  def processRegressRequest(): Unit = {

  }
}

