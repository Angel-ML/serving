package com.tencent.angel.serving.service.jersey.resources

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.google.protobuf.Int64Value
import com.google.protobuf.util.JsonFormat
import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.{GetModelStatusRequest, GetModelStatusResponse}
import com.tencent.angel.serving.apis.prediction.PredictProtos.{PredictRequest, PredictResponse}
import com.tencent.angel.serving.servables.angel.RunOptions
import com.tencent.angel.serving.servables.common.Predictor
import com.tencent.angel.serving.service.ModelServer
import com.tencent.angel.serving.service.common.GetModelStatusImpl
import com.tencent.angel.serving.service.jersey.util.ProcessInputOutput
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex


@Path("/")
class HttpRestApiHandler {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[ModelServer])

  @GET
  @Path("/angelServing/v1.0/models{requestPath:(?:/([^/:]+))?(?:/versions/(\\d+))?}")
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
        return Response.status(500).entity(errorMessage).build()
    }
    if(modelName == null || modelName.isEmpty) {
      val errorMessage = "Missing model name in request."
      LOG.info(errorMessage)
      Response.status(500).entity(errorMessage).build()
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
      val jsonFormat = JsonFormat.printer().print(builder.build())
      Response.status(200).entity(jsonFormat).build()
    }
  }

  @POST
  @Path("/angelServing/v1.0/models/{requestPath:([^/:]+)(?:/versions/(\\d+))?:(classify|regress|predict)}")
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
        return Response.status(500).entity(errorMessage).build()
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
      return Response.status(500).entity(errorMessage).build()
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
    val requestBuilder = PredictRequest.newBuilder()
    requestBuilder.setModelSpec(modelSpecBuilder.build())
    val format =ProcessInputOutput.fillPredictRequestFromJson(requestBody, requestBuilder, modelSpecBuilder)
    val responseBuilder = PredictResponse.newBuilder()
    val runOptions = new RunOptions()
    Predictor.predict(runOptions, ModelServer.getServerCore, requestBuilder.build(), responseBuilder)
    JSON.toJSONString(responseBuilder.build().getOutputs, SerializerFeature.EMPTY:_*)
  }

  def processRegressRequest(): Unit = {

  }
}

