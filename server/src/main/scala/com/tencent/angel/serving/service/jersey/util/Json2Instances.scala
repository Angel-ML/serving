package com.tencent.angel.serving.service.jersey.util

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.RequestProtos.Request
import com.tencent.angel.utils.ProtoUtils
import org.slf4j.{Logger, LoggerFactory}

import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object JsonPredictRequestFormat extends Enumeration {
  type JsonPredictRequestFormat = Value
  val kInvalid, kRow, kColumnar = Value
}


object ProcessInputOutput {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private val kPredictRequestInstancesKey = "instances"
  private val kBase64Key = "b64"

  def fillClassificationRequestFromJson(): Unit ={

  }

  def fillPredictRequestFromJson(requestBody: String, requestBuilder: Request.Builder,
                                 modelSpecBuilder: ModelSpec.Builder): Unit ={
    try {
      val jsonObject = parse(requestBody)
      val instances = jsonObject \ ProcessInputOutput.kPredictRequestInstancesKey
      if(!instances.isInstanceOf[JNothing.type]) {
        if(!instances.isInstanceOf[JArray] || instances.asInstanceOf[JArray].values.isEmpty) {
          LOG.info("Request data format error.")
          return
        }
        fillInstancesFromInstancesList(instances.asInstanceOf[JArray], requestBuilder)
      } else {
        LOG.info("Missing 'instances' key")
      }
    } catch {
      case ex: Exception =>
        LOG.info("Parse json string exception: " + ex)
    }
  }


  def fillInstancesFromInstancesList(instances: JArray, requestBuilder: Request.Builder): Unit = {
//    {"instances": [{"values": [1, 2, 3, 4], "key": 1}]}
//    可以省略命名
//    {
//      "instances": [
//      [0.0, 1.1, 2.2],
//      [3.3, 4.4, 5.5],
//      ...
//      ]
//    }
    val isElementObject = (value: Object) => {
      value.isInstanceOf[JObject] && !isValBase64Object(value)
    }
    val elementsAreObjects = isElementObject(instances(0))
    var instanceCount = 0
    for(ele <- instances.arr) {
      var instanceName: String = ""
      if(elementsAreObjects) {
        if(!isElementObject(ele)) {
          LOG.info("Expecting object but got list at item " + instanceCount + " of input list.")
          return
        }
        for((key, values) <- ele.asInstanceOf[JObject].obj) {
          if(!values.isInstanceOf[JArray] && !values.isInstanceOf[JObject]) {
            instanceName = values.toString
          } else {
            if(values.isInstanceOf[JArray]) {
              val instance = ProtoUtils.getInstance(instanceName, values.asInstanceOf[JArray].values.iterator)
              requestBuilder.addInstances(instance)
            } else if(values.isInstanceOf[JObject]) {
              val instance = ProtoUtils.getInstance(values.asInstanceOf[JObject].values.asJava)
              requestBuilder.addInstances(instance)
            }
          }
        }
      } else {
        if(isElementObject(ele)) {
          LOG.info("Expecting value/list but got object at item " + instanceCount + " of input list")
          return
        }
        if(ele.isInstanceOf[JArray]) {
          val instance = ProtoUtils.getInstance(ele.asInstanceOf[JArray].values.iterator)
          requestBuilder.addInstances(instance)
        }
      }
      instanceCount = instanceCount + 1
    }
  }

  def isValBase64Object(value: Object): Boolean ={
    value match {
      case nObject: JObject =>
        val base64String = nObject.values.get(kBase64Key)
        if (base64String.nonEmpty && nObject.values.size() == 1) {
          return true
        }
      case _ =>
    }
    false
  }

}


