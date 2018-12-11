package com.tencent.angel.serving.service.jersey.util

import com.alibaba.fastjson.util.TypeUtils
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.google.protobuf.ByteString
import com.tencent.angel.core.graph.TensorProtos.TensorProto
import com.tencent.angel.core.graph.TensorShapeProtos.TensorShapeProto
import com.tencent.angel.core.graph.TypesProtos.DataType
import com.tencent.angel.core.graph.TypesProtos.DataType._
import com.tencent.angel.core.saver.MetaGraphProtos.TensorInfo
import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest
import com.tencent.angel.serving.core.{ServableHandle, ServableRequest, ServerCore}
import com.tencent.angel.serving.servables.angel.SavedModelBundle
import com.tencent.angel.serving.service.ModelServer
import com.tencent.angel.serving.service.jersey.util.JsonPredictRequestFormat.JsonPredictRequestFormat
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable

object JsonPredictRequestFormat extends Enumeration {
  type JsonPredictRequestFormat = Value
  val kInvalid, kRow, kColumnar = Value
}

object ProcessInputOutput {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private val kPredictRequestSignatureKey = "signature_name"
  private val kPredictRequestInstancesKey = "instances"
  private val kPredictRequestInputsKey = "inputs"
  private val kClassifyRegressRequestContextKey = "context"
  private val kClassifyRegressRequestExamplesKey = "examples"
  private val kPredictResponsePredictionsKey = "predictions"
  private val kPredictResponseOutputsKey = "outputs"
  private val kClassifyRegressResponseKey = "results"
  private val kBase64Key = "b64"
  private val kBytesTensorNameSuffix = "_bytes"

  def fillClassificationRequestFromJson(): Unit ={

  }

  def fillPredictRequestFromJson(requestBody: String, requestBuilder: PredictRequest.Builder,
                                 modelSpecBuilder: ModelSpec.Builder): JsonPredictRequestFormat ={
    var format = JsonPredictRequestFormat.kInvalid
    val jsonObject = JSON.parseObject(requestBody)
    val signatureName = jsonObject.getString(kPredictRequestSignatureKey)
    if(signatureName != null && !signatureName.isEmpty) {
        modelSpecBuilder.setSignatureName(signatureName)
    }
    val infoMap = getInfoMap(ModelServer.getServerCore, modelSpecBuilder.build())
    val instances = jsonObject.getJSONArray(kPredictRequestInstancesKey)
    val inputs = jsonObject.get(kPredictRequestInputsKey)
    if(instances != null) {
      if(inputs != null || !instances.isInstanceOf[JSONArray] || instances.asInstanceOf[JSONArray].isEmpty) {
        LOG.info("Request data format error.")
      }
      format = JsonPredictRequestFormat.kRow
      fillTensorMapFromInstancesList(instances, infoMap, requestBuilder)
    } else if(inputs != null) {
      if(instances != null) {
        LOG.info("Request data format error.")
      }
      format = JsonPredictRequestFormat.kColumnar
      fillTensorMapFromInputsMap(inputs, infoMap, requestBuilder)
    } else {
      LOG.info("Missing 'inputs' or 'instances' key")
    }
    format
  }

  def fillRegressionRequestFromJson(): Unit ={

  }

  //Get signature def according signature name
  def getInfoMap(core:ServerCore, modelSpec: ModelSpec): Map[String, TensorInfo] = {
    val servableHandle: ServableHandle[SavedModelBundle] = core.servableHandle(ServableRequest
      .specific(modelSpec.getName, modelSpec.getVersion.getValue))
    val signatureName: String = if (modelSpec.getSignatureName.isEmpty) "defaultServing" else modelSpec.getSignatureName
    //todo, now fake data
    val featuresDim1: TensorShapeProto.Dim = TensorShapeProto.Dim.newBuilder.setSize(1).build
    val featuresDim2: TensorShapeProto.Dim = TensorShapeProto.Dim.newBuilder.setSize(5).build
    val featuresShape = TensorShapeProto.newBuilder.addDim(featuresDim1).addDim(featuresDim2).build
    val infoMap = new mutable.HashMap[String, TensorInfo]()
    val tensorInfo = TensorInfo.newBuilder().setDtype(DT_FLOAT)
      .setTensorShape(featuresShape).setName("input:0").build()
    infoMap.put("input", tensorInfo)
    infoMap.toMap
  }

  def fillTensorMapFromInstancesList(instances: JSONArray, infoMap: Map[String, TensorInfo],
                                     requestBuilder: PredictRequest.Builder): Unit = {
    //instances is JSONArray, example:
    //     [
    //       {
    //         "tag": ["foo"],
    //         "signal": [1, 2, 3, 4, 5],
    //         "sensor": [[1, 2], [3, 4]]
    //       },
    //       {
    //         "tag": ["bar"],
    //         "signal": [3, 4, 1, 2, 5],
    //         "sensor": [[4, 5], [6, 8]]
    //       }
    //     ]
    if(!instances.get(0).isInstanceOf[JSONObject] && infoMap.size > 1) {
      LOG.info("instances is a plain list, but expecting list of objects as multiple " +
        "input tensors required as per tensorinfo map")
      return
    }
    val isElementObject = (value: Object) => {
      value.isInstanceOf[JSONObject] && !isValBase64Object(value)
    }
    val elementsAreObjects = isElementObject(instances.get(0))
    val inputNames = infoMap.keys
    val tensorBuilderMap = new mutable.HashMap[String, TensorProto.Builder]()
    val sizeMap = new mutable.HashMap[String, Int]()
    val shapeMap = new mutable.HashMap[String, TensorShapeProto]()
    var tensorCount = 0
    for(ele <- instances) {
      if(elementsAreObjects) {
        if(!isElementObject(ele)) {
          LOG.info("Expecting object but got list at item " + tensorCount + " of input list.")
          return
        }
        val objectsKeys = ele.asInstanceOf[JSONObject].keys
        for((k, v) <- ele.asInstanceOf[JSONObject]) {
          addInstanceItem(v, k, infoMap, sizeMap, shapeMap, tensorBuilderMap)
        }
        if(!objectsKeys.equals(inputNames)) {
          LOG.info("Failed to process element: " + tensorCount + " of 'instances' list. JSON object: " +
          ele.toString + " must only have keys: " + inputNames.mkString(","))
          return
        }
      } else {
        if(isElementObject(ele)) {
          LOG.info("Expecting value/list but got object at item " + tensorCount + " of input list")
          return
        }
        val name = infoMap.iterator.next()._1
        addInstanceItem(ele, name, infoMap, sizeMap, shapeMap, tensorBuilderMap)
      }
      tensorCount = tensorCount + 1
    }
    val tenorMap = new mutable.HashMap[String, TensorProto]()
    for((name, tensorBuilder) <- tensorBuilderMap) {
      tensorBuilder.setDtype(infoMap(name).getDtype)
      val shape = shapeMap(name)
      val tensorShapeBuilder = TensorShapeProto.newBuilder()
      tensorShapeBuilder.addDim(TensorShapeProto.Dim.newBuilder().setSize(tensorCount))
      shape.getDimList.map{ x =>
        tensorShapeBuilder.addDim(TensorShapeProto.Dim.newBuilder().setSize(x.getSize))
      }
      tensorBuilder.setTensorShape(tensorShapeBuilder.build())
      tenorMap.put(name, tensorBuilder.build())
    }
    requestBuilder.putAllInputs(tenorMap)
  }

  def fillTensorMapFromInputsMap(inputs: Object, infoMap: Map[String, TensorInfo],
                                 requestBuilder: PredictRequest.Builder): Unit = {
    if(!inputs.isInstanceOf[JSONObject] || isValBase64Object(inputs)) {
      if(infoMap.size > 1) {
        LOG.info("inputs is a plain value/list, but expecting an object as multiple" +
          " input tensors required as per tensor info map")
        return
      }
      val tensorBuilder = TensorProto.newBuilder()
      tensorBuilder.setDtype(infoMap.iterator.next()._2.getDtype)
      val tensorShapeBuilder = TensorShapeProto.newBuilder()
      getDenseTensorShape(inputs, tensorShapeBuilder)
      tensorBuilder.setTensorShape(tensorShapeBuilder.build())
      val unused = fillTensorProto(inputs, 0, tensorBuilder.getDtype, 0, tensorBuilder)
      val tenorMap = new mutable.HashMap[String, TensorProto]()
      tenorMap.put(infoMap.iterator.next()._1, tensorBuilder.build())
      requestBuilder.putAllInputs(tenorMap)
    } else {
      val tenorMap = new mutable.HashMap[String, TensorProto]()
      for((name, tensorInfo) <- infoMap) {
        val item = inputs.asInstanceOf[JSONObject].get(name)
        if(item == null) {
          LOG.info("Missing named input: " + name + " in 'inputs' object.")
          return
        }
        val dtype = tensorInfo.getDtype
        val tensorBuilder = TensorProto.newBuilder()
        tensorBuilder.setDtype(dtype)
        val tensorShapeBuilder = TensorShapeProto.newBuilder()
        getDenseTensorShape(item, tensorShapeBuilder)
        tensorBuilder.setTensorShape(tensorShapeBuilder.build())
        val unused = fillTensorProto(item, 0, dtype, 0, tensorBuilder)
        tenorMap.put(name, tensorBuilder.build())
      }
      requestBuilder.putAllInputs(tenorMap)
    }
  }

  def isValBase64Object(value: Object): Boolean ={
    value match {
      case nObject: JSONObject =>
        val base64String = nObject.getString(kBase64Key)
        if (base64String != null && nObject.size() == 1) {
          return true
        }
      case _ =>
    }
    false
  }

  def addInstanceItem(value: Object, name: String, tensorInfoMap: Map[String, TensorInfo],
                      sizeMap: mutable.HashMap[String, Int],
                      shapeMap: mutable.HashMap[String, TensorShapeProto],
                      tensorMap: mutable.HashMap[String, TensorProto.Builder]): Unit ={
    if(!tensorInfoMap.contains(name)){
      LOG.info("JSON object: does not have named input: " + name)
      return
    }
    val dtype = tensorInfoMap(name).getDtype
    val tensorShapeProtoBuilder = TensorShapeProto.newBuilder()
    getDenseTensorShape(value, tensorShapeProtoBuilder)
    val tensorShapeProto = tensorShapeProtoBuilder.build()
    val tensorProtoBuilder = TensorProto.newBuilder()
    tensorProtoBuilder.setTensorShape(tensorShapeProto)
    val size = fillTensorProto(value, 0, dtype, 0, tensorProtoBuilder)
    tensorMap.put(name, tensorProtoBuilder)
    if(!sizeMap.contains(name)) {
      sizeMap.put(name, size)
      shapeMap.put(name, tensorShapeProto)
    } else if(sizeMap(name) != size) {
      LOG.info("Expecting tensor size: " + sizeMap.get(name) + " but got: " + size)
      return
    } else if(!shapeMap(name).equals(tensorShapeProto)) {
      LOG.info("Expecting shape: " + sizeMap(name).toString + " but got: " + tensorShapeProto.toString)
      return
    }
  }

  def getDenseTensorShape(value: Object, tensorShapeProtoBuilder: TensorShapeProto.Builder): Unit = {
    if(!value.isInstanceOf[JSONArray]) return
    val size = value.asInstanceOf[JSONArray].size()
    tensorShapeProtoBuilder.addDim(TensorShapeProto.Dim.newBuilder.setSize(size).build)
    if(size > 0) {
      getDenseTensorShape(value.asInstanceOf[JSONArray].get(0), tensorShapeProtoBuilder)
    }
  }

  def fillTensorProto(value: Object, level: Int, dtype: DataType,
                      size: Int, tensorProtoBuilder: TensorProto.Builder): Int ={
    val rank = tensorProtoBuilder.getTensorShape.getDimCount
    var valueCount = size
    if(!value.isInstanceOf[JSONArray]) {
      if(level != rank) {
        LOG.info("JSON Value: " + value.toString + "found at incorrect level: " + (level+1) +
          "in the JSON DOM. Expected at level: " + rank)
        return -1
      }
      value match {
        case nObject: JSONObject =>
          if (dtype == DT_STRING) {
            if (!isValBase64Object(value)) {
              LOG.info("JSON Value: " + value.toString + " not formatted correctly for base64 data.")
              return -1
            }
            val base64Str = nObject.getString(kBase64Key)
            tensorProtoBuilder.addStringVal(ByteString.copyFrom(base64Str, "UTF-8"))
          } else {
            LOG.info("JSON Value: " + value.toString + "expect Type: "
              + DT_STRING + " but type: " + dtype.toString)
            return -1
          }
        case _ =>
          addValueToTensor(value, dtype, tensorProtoBuilder)
      }
      valueCount = valueCount + 1
      return -1
    }
    if(level >= rank) {
      LOG.info("Encountered list at unexpected level: " + level + " expected < " + rank)
      return -1
    }
    if(!value.asInstanceOf[JSONArray].size().toLong.
      equals(tensorProtoBuilder.getTensorShape.getDim(level).getSize)) {
      LOG.info("Encountered list at unexpected size: " + value.asInstanceOf[JSONArray].size().toLong +
      " at level: " + level + " expected size: " + tensorProtoBuilder.getTensorShape.getDim(level).getSize)
      return -1
    }
    value.asInstanceOf[JSONArray].foreach{ x =>
      fillTensorProto(x, level+1, dtype, valueCount, tensorProtoBuilder)
    }
    valueCount
  }

  def addValueToTensor(value: Object, dtype: DataType, tensorProtoBuilder: TensorProto.Builder): Unit ={
    dtype match {
      case DT_FLOAT =>
        tensorProtoBuilder.addFloatVal(TypeUtils.castToFloat(value))
      case DT_DOUBLE =>
        tensorProtoBuilder.addDoubleVal(TypeUtils.castToDouble(value))
      case DT_INT32 | DT_INT16 | DT_INT8 | DT_UINT8 =>
        tensorProtoBuilder.addIntVal(TypeUtils.castToInt(value))
      case DT_STRING =>
        tensorProtoBuilder.addStringVal(ByteString.copyFrom(TypeUtils.castToBytes(value)))
      case DT_INT64 =>
        tensorProtoBuilder.addInt64Val(TypeUtils.castToLong(value))
      case DT_BOOL =>
        tensorProtoBuilder.addBoolVal(TypeUtils.castToBoolean(value))
      case DT_UINT32 =>
        tensorProtoBuilder.addUint32Val(TypeUtils.castToInt(value))
      case DT_UINT64 =>
        tensorProtoBuilder.addUint64Val(TypeUtils.castToLong(value))
      case _ =>
        LOG.info("Conversion of JSON Value: " + value.toString + " to type: " + dtype)
    }
  }
//
//  def makeJsonFromTensors(tensorMap: Map[String, TensorProto], format: JsonPredictRequestFormat): String ={
//    format match {
//      case JsonPredictRequestFormat.kInvalid =>
//        LOG.info("Invalid request format")
//        return null
//      case JsonPredictRequestFormat.kRow =>
//
//    }
//  }

}

