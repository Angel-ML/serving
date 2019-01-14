package com.tencent.angel.serving.service.jersey.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.prediction.RequestProtos.Request
import com.tencent.angel.utils.ProtoUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

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
    val jsonObject = JSON.parseObject(requestBody)
    val instances = jsonObject.getJSONArray(kPredictRequestInstancesKey)
    if(instances != null) {
      if(!instances.isInstanceOf[JSONArray] || instances.asInstanceOf[JSONArray].isEmpty) {
        LOG.info("Request data format error.")
      }
      fillInstancesFromInstancesList(instances, requestBuilder)
    } else {
      LOG.info("Missing 'instances' key")
    }
  }


  def fillInstancesFromInstancesList(instances: JSONArray, requestBuilder: Request.Builder): Unit = {
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
      value.isInstanceOf[JSONObject] && !isValBase64Object(value)
    }
    val elementsAreObjects = isElementObject(instances.get(0))
    var instanceCount = 0
    for(ele <- instances) {
      var instanceName: String = ""
      if(elementsAreObjects) {
        if(!isElementObject(ele)) {
          LOG.info("Expecting object but got list at item " + instanceCount + " of input list.")
          return
        }
        for((k, v) <- ele.asInstanceOf[JSONObject]) {
          if(!v.isInstanceOf[JSONArray] && !v.isInstanceOf[JSONObject]) {
            instanceName = v.toString
          } else {
            if(v.isInstanceOf[JSONArray]) {
              if(v.asInstanceOf[JSONArray].get(0).isInstanceOf[JSONArray]) {
                if(v.asInstanceOf[JSONArray].get(0).asInstanceOf[JSONArray].get(0).isInstanceOf[JSONArray]) {
                  val values = v.asInstanceOf[JSONArray]
                  val numRows = values.size()
                  val numCols = values.get(0).asInstanceOf[JSONArray].size()
                  val numChannel = values.get(0).asInstanceOf[JSONArray].get(0).asInstanceOf[JSONArray].size()
                  val instance = ProtoUtils.getInstance(numRows, numCols, numChannel, values.iterator())
                  requestBuilder.addInstances(instance)
                } else {
                  val values = v.asInstanceOf[JSONArray]
                  val numRows = values.size()
                  val numCols = values.get(0).asInstanceOf[JSONArray].size()
                  val instance = ProtoUtils.getInstance(numRows, numCols, values.iterator())
                  requestBuilder.addInstances(instance)
                }
              } else {
                val instance = ProtoUtils.getInstance(instanceName, v.asInstanceOf[JSONArray].iterator())
                requestBuilder.addInstances(instance)
              }
            } else if(v.isInstanceOf[JSONObject]) {
              val instance = ProtoUtils.getInstance(v.asInstanceOf[JSONObject])
              requestBuilder.addInstances(instance)
            }
          }
        }

      } else {
        if(isElementObject(ele)) {
          LOG.info("Expecting value/list but got object at item " + instanceCount + " of input list")
          return
        }
        if(ele.isInstanceOf[JSONArray]) {
          if(ele.asInstanceOf[JSONArray].get(0).isInstanceOf[JSONArray]) {
            if(ele.asInstanceOf[JSONArray].get(0).asInstanceOf[JSONArray].get(0).isInstanceOf[JSONArray]) {
              val values = ele.asInstanceOf[JSONArray]
              val numRows = values.size()
              val numCols = values.get(0).asInstanceOf[JSONArray].size()
              val numChannel = values.get(0).asInstanceOf[JSONArray].get(0).asInstanceOf[JSONArray].size()
              val instance = ProtoUtils.getInstance(numRows, numCols, numChannel, values.iterator())
              requestBuilder.addInstances(instance)
            } else {
              val values = ele.asInstanceOf[JSONArray]
              val numRows = values.size()
              val numCols = values.get(0).asInstanceOf[JSONArray].size()
              val instance = ProtoUtils.getInstance(numRows, numCols, values.iterator())
              requestBuilder.addInstances(instance)
            }
          } else {
            val instance = ProtoUtils.getInstance(ele.asInstanceOf[JSONArray].iterator())
            requestBuilder.addInstances(instance)
          }
        }
      }
      instanceCount = instanceCount + 1
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

}


