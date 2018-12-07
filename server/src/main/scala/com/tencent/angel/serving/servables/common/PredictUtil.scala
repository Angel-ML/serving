package com.tencent.angel.serving.servables.common

import com.google.protobuf.Int64Value
import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef
import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.PredictProtos.{PredictRequest, PredictResponse}
import com.tencent.angel.serving.servables.angel.{RunOptions, Session}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

object PredictUtil{

  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  def runPredict(runOptions: RunOptions, metaGraphDef: MetaGraphDef, servableVersion: Long, session: Session,
                 request: PredictRequest, responseBuilder: PredictResponse.Builder): Unit ={
    //todo
    val signatureName: String = if (request.getModelSpec.getSignatureName.isEmpty) "defaultServing" else request.getModelSpec.getSignatureName
    //val signature: SignatureDef  = metaGraphDef.getSignatureDefMap.get(signatureName)
    val modelSpec: ModelSpec = ModelSpec.newBuilder().clear().setName(request.getModelSpec.getName)
      .setSignatureName(signatureName).setVersion(Int64Value.newBuilder().setValue(servableVersion).build()).build()
    responseBuilder.setModelSpec(modelSpec)
    //val outputTensorNames = new ListBuffer[String]
    //val outputTensors = new ListBuffer[TensorProto]
    for((inputName, tensor) <- request.getInputsMap) {
      //outputTensorNames.append(inputName + "Result")
      //outputTensors.append(tensor)
      responseBuilder.putOutputs(inputName + "Result", tensor)
    }
  }
}
