package com.tencent.angel.serving.service.jersey.resources

import com.google.protobuf.Int64Value
import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.{GetModelStatusRequest, GetModelStatusResponse}
import com.tencent.angel.serving.service.ModelServer
import com.tencent.angel.serving.service.common.GetModelStatusImpl
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex

@Path("/")
class HttpRestApiHandler {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[ModelServer])

  @GET
  @Path("/angelServing/v1.0/models{requestPath:(?:/([^/:]+))?(?:/versions/(\\d+))?}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def processModelServiceRequest(@PathParam("requestPath") requestPath: String): String = {
    val pattern = new Regex("""(?:/([^/:]+))?(?:/versions/(\d+))?""")
    val pattern(_, _) = requestPath
    var modelName: String = null
    var modelVersion: String = null
    requestPath match {
      case pattern(name, version) =>
        modelName = name
        modelVersion = version
      case _ =>
        LOG.info("unsupported request url.")
    }
    if(modelName == null || modelName.isEmpty) {
      LOG.info("Missing model name in request.")
      null
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
      builder.build().toString
    }
  }

  @POST
  @Path("/angelServing/v1.0/models/{requestPath:([^/:]+)(?:/versions/(\\d+))?:(classify|regress|predict)}")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def processPredictionServiceRequest(requestBody: String, @PathParam("requestPath") requestPath: String): String ={
    val pattern = new Regex("""([^/:]+)(?:/versions/(\d+))?:(classify|regress|predict)""")
    val pattern(_, _, _) = requestPath
    var modelName: String = null
    var modelVersion: String = null
    var modelMethod: String = null
    requestPath match {
      case pattern(name, version, method) =>
        modelName = name
        modelVersion = version
        modelMethod = method
      case _ =>
        LOG.info("unsupported request url.")
    }
    if(modelMethod.equals("classify")) {
      processClassifyRequest
    } else if(modelMethod.equals("predict")) {
      processClassifyRequest
    } else if(modelMethod.equals("regress")) {
      processRegressRequest
    } else {
      return "error"
    }
    "ok"
  }

  def processClassifyRequest: Unit = {

  }

  def processPredictRequest: Unit = {

  }

  def processRegressRequest: Unit = {

  }

  def fillClassificationRequestFromJson(): Unit ={

  }

  def fillPredictRequestFromJson: Unit ={

  }

  def fillRegressionRequestFromJson: Unit ={

  }


}

