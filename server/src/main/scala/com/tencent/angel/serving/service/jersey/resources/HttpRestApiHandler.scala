package com.tencent.angel.serving.service.jersey.resources

import com.google.protobuf.Int64Value
import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.{GetModelStatusRequest, GetModelStatusResponse}
import com.tencent.angel.serving.service.{GetModelStatusImpl, ModelServer}
import javax.ws.rs._
import javax.ws.rs.core.MediaType

@Path("/")
class HttpRestApiHandler {
  @GET
  @Path("/angelServing/v1.0/models/{modelName}/")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def processModelServiceRequest(@PathParam("modelName") modelName: String): String = {
    if(modelName.isEmpty) {
      System.out.print("Missing model name in request.")
      return null
    }
    val modelSpec = ModelSpec.newBuilder().setName(modelName)
    val request = GetModelStatusRequest.newBuilder().setModelSpec(modelSpec).build()
    val builder = GetModelStatusResponse.newBuilder()
    GetModelStatusImpl.getModelStatus(ModelServer.getServerCore, request, builder)
    builder.build().toString
  }


  @GET
  @Path("/angelServing/v1.0/models/{modelName}/versions/{modelVersion}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def processModelServiceRequestWithVersion(@PathParam("modelName") modelName: String,
                                            @PathParam("modelVersion") modelVersion: String): String = {
    if(modelName.isEmpty) {
      System.out.print("Missing model name in request.")
      return null
    }
    val modelSpec = ModelSpec.newBuilder().setName(modelName).setVersion(Int64Value.newBuilder().setValue(modelVersion.toLong))
    val request = GetModelStatusRequest.newBuilder().setModelSpec(modelSpec).build()
    val builder = GetModelStatusResponse.newBuilder()
    GetModelStatusImpl.getModelStatus(ModelServer.getServerCore, request, builder)
    builder.build().toString
  }

  @POST
  @Path("/angelServing/v1.0/models/{modelName}/methods/{modelMethod}")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def processPredictionServiceRequest(requestBody: String, @PathParam("modelName") modelName: String,
                                      @PathParam("modelMethod") modelMethod: String): String ={
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

  @POST
  @Path("/angelServing/v1.0/models/{modelName}/methods/{modelMethod}")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def processPredictionServiceRequestWithVersion(requestBody: String, @PathParam("modelName") modelName: String,
                                                 @PathParam("modelVersion") modelVersion: String,
                                                 @PathParam("modelMethod") modelMethod: String): String ={
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

