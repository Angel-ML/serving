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
package com.tencent.angel.serving.service.jersey.util

import com.tencent.angel.serving.apis.common.TypesProtos
import com.tencent.angel.serving.apis.prediction.RequestProtos.Request
import com.tencent.angel.serving.servables.common.SavedModelBundle
import com.tencent.angel.serving.service.ModelServer
import com.tencent.angel.utils.ProtoUtils
import org.apache.jute.compiler.{JFloat, JLong}
import org.apache.spark.ml.linalg.Vectors
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object Json2Instances {

  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private val kPredictRequestInstancesKey = "instances"
  private val sparseKey = "sparseIndices"
  private val sparseValue = "sparseValues"
  private val kBase64Key = "b64"

  def fillClassificationRequestFromJson(): Unit = {

  }

  def fillPredictRequestFromJson(requestBody: String, requestBuilder: Request.Builder): Unit = {
    try {
      val jsonObject = parse(requestBody)
      val instances = jsonObject \ Json2Instances.kPredictRequestInstancesKey
      if (!instances.isInstanceOf[JNothing.type]) {
        if (!instances.isInstanceOf[JArray] || instances.asInstanceOf[JArray].values.isEmpty) {
          throw new Exception("Request data format error.")
        }
        fillInstancesFromInstancesList(instances.asInstanceOf[JArray], requestBuilder)
      } else {
        throw new Exception("Missing 'instances' key")
      }
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }


  def fillInstancesFromInstancesList(instances: JArray, requestBuilder: Request.Builder): Unit = {
    //fill instance from json data, read docs/restful-api.md to know details data format.
    val servableRequest = ModelServer.getServerCore.servableRequestFromModelSpec(requestBuilder.getModelSpec)
    val servableHandle = ModelServer.getServerCore.servableHandle[SavedModelBundle](servableRequest)
    val (keyType, valueType, dim) = servableHandle.servable.getInputInfo()
    val isElementObject = (value: Object) => {
      value.isInstanceOf[JObject] && !isValBase64Object(value)
    }
    val elementsAreObjects = isElementObject(instances(0))
    var instanceCount = 0
    for (ele <- instances.arr) {
      if (elementsAreObjects) {
        if (!isElementObject(ele)) {
          throw new Exception("Expecting object but got list at item " + instanceCount + " of input list.")
        }
        if (dim == -10) {
          //spark prediction data parse
          if (ele.asInstanceOf[JObject].values.size < 2) {
            var instanceName: String = ""
            var examples: Map[String, Any] = null
            for ((key, values) <- ele.asInstanceOf[JObject].obj) {
              values match {
                case jObject: JObject =>
                  jObject.asInstanceOf[JObject] \ Json2Instances.sparseKey match {
                    case JNothing =>
                      jObject.asInstanceOf[JObject] \ Json2Instances.sparseValue match {
                        case arrValues: JArray =>
                          keyType match {
                            case TypesProtos.DataType.DT_INT32 =>
                              var example: List[Any] = null
                              valueType match {
                                case TypesProtos.DataType.DT_DOUBLE =>
                                  example = arrValues.values.map {
                                    case v: Double => v
                                    case v: Float => v.toDouble
                                    case v: Long => v.toDouble
                                    case v: Int => v.toDouble
                                    case v: String => v.toDouble
                                    case v: BigInt => v.toDouble
                                    case v =>
                                      throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                                  }
                                case TypesProtos.DataType.DT_STRING =>
                                  example = arrValues.values.map {
                                    case v: Double => v.toString
                                    case v: Float => v.toString
                                    case v: Long => v.toString
                                    case v: Int => v.toString
                                    case v: String => v
                                    case v: BigInt => v.toString
                                    case v =>
                                      throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                                  }
                                case TypesProtos.DataType.DT_INT32 =>
                                  example = arrValues.values.map {
                                    case v: Double => v.toInt
                                    case v: Float => v.toInt
                                    case v: Long => v.toInt
                                    case v: Int => v
                                    case v: String => v.toInt
                                    case v: BigInt => v.toInt
                                    case v =>
                                      throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                                  }
                                case _ => new Exception("unsuported data type!")
                              }
                              val indices = (0 until example.length).map(_.toInt.asInstanceOf[java.lang.Integer]).toList
                              val res = Map[String, java.util.Map[java.lang.Integer, Any]](key -> indices.zip(example).toMap.asJava)
                              val instance = ProtoUtils.getInstance(res.asJava)
                              println("instance type: ", instance.getDType, instance.getValueCase)
                              requestBuilder.addInstances(instance)
                            case _ => new Exception("unsuported key data type!")
                          }
                        case _ => throw new Exception("Json format error!")
                      }
                    case arrIndices: JArray =>
                      val d = jObject.asInstanceOf[JObject] \ "dim" match {
                        case jInt: JInt => jInt.values.toInt
                        case jDouble: JInt => jDouble.values.toInt
                        case jFloat: JInt => jFloat.values.toInt
                        case jString: JInt => jString.values.toInt
                        case jLong: JInt => jLong.values.toInt
                      }
                      jObject.asInstanceOf[JObject] \ Json2Instances.sparseValue match {
                        case arrValues: JArray =>
                          keyType match {
                            case TypesProtos.DataType.DT_INT32 =>
                              val indices = arrIndices.values.map(x => x.asInstanceOf[BigInt].toInt.asInstanceOf[java.lang.Integer]).toArray
                              var example: Array[Any] = null
                              valueType match {
                                case TypesProtos.DataType.DT_DOUBLE =>
                                  example = arrValues.values.map {
                                    case v: Double => v
                                    case v: Float => v.toDouble
                                    case v: Long => v.toDouble
                                    case v: Int => v.toDouble
                                    case v: String => v.toDouble
                                    case v: BigInt => v.toDouble
                                    case v =>
                                      throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                                  }.toArray
                                case TypesProtos.DataType.DT_STRING =>
                                  example = arrValues.values.map {
                                    case v: Double => v.toString
                                    case v: Float => v.toString
                                    case v: Long => v.toString
                                    case v: Int => v.toString
                                    case v: String => v
                                    case v: BigInt => v.toString
                                    case v =>
                                      throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                                  }.toArray
                                case TypesProtos.DataType.DT_INT32 =>
                                  example = arrValues.values.map {
                                    case v: Double => v.toInt
                                    case v: Float => v.toInt
                                    case v: Long => v.toInt
                                    case v: Int => v
                                    case v: String => v.toInt
                                    case v: BigInt => v.toInt
                                    case v =>
                                      throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                                  }.toArray
                                case _ => new Exception("unsuported data type!")
                              }
                              val res = (Map[String, java.util.Map[java.lang.Integer, Any]](key -> indices.zip(example).toMap.asJava)).asJava
                              val instance = ProtoUtils.getInstance(res, d.toInt)
                              requestBuilder.addInstances(instance)
                            case TypesProtos.DataType.DT_INT64 =>
                              val indices = arrIndices.values.map(x => x.asInstanceOf[BigInt].toInt.asInstanceOf[java.lang.Integer]).toArray
                              var example: Array[Any] = null
                              valueType match {
                                case TypesProtos.DataType.DT_DOUBLE =>
                                  example = arrValues.values.map {
                                    case v: Double => v
                                    case v: Float => v.toDouble
                                    case v: Long => v.toDouble
                                    case v: Int => v.toDouble
                                    case v: String => v.toDouble
                                    case v: BigInt => v.toDouble
                                    case v =>
                                      throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                                  }.toArray
                                case TypesProtos.DataType.DT_STRING =>
                                  example = arrValues.values.map {
                                    case v: Double => v.toString
                                    case v: Float => v.toString
                                    case v: Long => v.toString
                                    case v: Int => v.toString
                                    case v: String => v
                                    case v: BigInt => v.toString
                                    case v =>
                                      throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                                  }.toArray
                                case TypesProtos.DataType.DT_INT32 =>
                                  example = arrValues.values.map {
                                    case v: Double => v.toInt
                                    case v: Float => v.toInt
                                    case v: Long => v.toInt
                                    case v: Int => v
                                    case v: String => v.toInt
                                    case v: BigInt => v.toInt
                                    case v =>
                                      throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                                  }.toArray
                                case _ => new Exception("unsuported data type!")
                              }
                              val res = Map[String, java.util.Map[java.lang.Integer, Any]](key -> indices.zip(example).toMap.asJava)
                              val instance = ProtoUtils.getInstance(res.asJava, d.toInt)
                              requestBuilder.addInstances(instance)
                            case _ => new Exception("unsuported key data type!")
                          }
                        case _ => throw new Exception("Json format error!")
                      }
                    case _ => throw new Exception("Json format error!")
                  }
                case arrIndices: JArray =>

                  ele.asInstanceOf[JObject] \ Json2Instances.sparseValue match {
                    case JNothing =>
                      var example: List[Any] = null
                      valueType match {
                        case TypesProtos.DataType.DT_DOUBLE =>
                          example = arrIndices.values.map{
                            case v: Double => v
                            case v: Float => v.toDouble
                            case v: Long => v.toDouble
                            case v: Int => v.toDouble
                            case v: String => v.toDouble
                            case v: BigInt => v.toDouble
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                          }
                        case TypesProtos.DataType.DT_STRING =>
                          example = arrIndices.values.map{
                            case v: Double => v.toString
                            case v: Float => v.toString
                            case v: Long => v.toString
                            case v: Int => v.toString
                            case v: String => v
                            case v: BigInt => v.toString
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                          }
                        case TypesProtos.DataType.DT_INT32 =>
                          example = arrIndices.values.map {
                            case v: Double => v.toInt
                            case v: Float => v.toInt
                            case v: Long => v.toInt
                            case v: Int => v
                            case v: String => v.toInt
                            case v: BigInt => v.toInt
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                          }
                        case _ => new Exception("unsuported data type!")
                      }
                      val res = Map[String, Any](key -> example.asJava)
                      val instance = ProtoUtils.getInstance(res.asJava)
                      requestBuilder.addInstances(instance)

                    case arrValues: JArray =>
                      keyType match {
                        case TypesProtos.DataType.DT_INT32 =>
                          val indices = arrIndices.values.map(x => x.asInstanceOf[BigInt].toInt.asInstanceOf[java.lang.Integer])
                          var example: List[Any] = null
                          valueType match {
                            case TypesProtos.DataType.DT_DOUBLE =>
                              example = arrValues.values.map {
                                case v: Double => v
                                case v: Float => v.toDouble
                                case v: Long => v.toDouble
                                case v: Int => v.toDouble
                                case v: String => v.toDouble
                                case v: BigInt => v.toDouble
                                case v =>
                                  throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                              }
                            case TypesProtos.DataType.DT_STRING =>
                              example = arrValues.values.map{
                                case v: Double => v.toString
                                case v: Float => v.toString
                                case v: Long => v.toString
                                case v: Int => v.toString
                                case v: String => v
                                case v: BigInt => v.toString
                                case v =>
                                  throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                              }
                            case TypesProtos.DataType.DT_INT32 =>
                              example = arrValues.values.map {
                                case v: Double => v.toInt
                                case v: Float => v.toInt
                                case v: Long => v.toInt
                                case v: Int => v
                                case v: String => v.toInt
                                case v: BigInt => v.toInt
                                case v =>
                                  throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                              }
                            case _ => new Exception("unsuported data type!")
                          }
                          val instance = ProtoUtils.getInstance(dim.asInstanceOf[Int], indices.zip(example).toMap.asJava)
                          requestBuilder.addInstances(instance)
                        case TypesProtos.DataType.DT_INT64 =>
                          val indices = arrIndices.values.map(x => x.asInstanceOf[BigInt].toLong.asInstanceOf[java.lang.Long])
                          var example: List[Any] = null
                          valueType match {
                            case TypesProtos.DataType.DT_DOUBLE =>
                              example = arrValues.values.map {
                                case v: Double => v
                                case v: Float => v.toDouble
                                case v: Long => v.toDouble
                                case v: Int => v.toDouble
                                case x: String => x.toDouble
                                case v: BigInt => v.toDouble
                                case v =>
                                  throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                              }
                            case TypesProtos.DataType.DT_STRING =>
                              example = arrValues.values.map{
                                case v: Double => v.toString
                                case v: Float => v.toString
                                case v: Long => v.toString
                                case v: Int => v.toString
                                case v: String => v
                                case v: BigInt => v.toString
                                case v =>
                                  throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                              }
                            case TypesProtos.DataType.DT_INT32 =>
                              example = arrValues.values.map {
                                case v: Double => v.toInt
                                case v: Float => v.toInt
                                case v: Long => v.toInt
                                case v: Int => v
                                case v: String => v.toInt
                                case v: BigInt => v.toInt
                                case v =>
                                  throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                              }
                            case _ => new Exception("unsuported data type!")
                          }
                          val instance = ProtoUtils.getInstance(dim.asInstanceOf[Long], indices.zip(example).toMap.asJava)
                          requestBuilder.addInstances(instance)
                        case _ => new Exception("unsuported key data type!")
                      }
                    case _ => throw new Exception("Json format error!")
                  }
                case jInt: JInt =>
                  var example: Any = null
                  valueType match {
                    case TypesProtos.DataType.DT_DOUBLE =>
                      example = jInt.values.toDouble
                    case _ => new Exception("unsuported data type!")
                  }
                  val res = Map[String, Any](key -> example)
                  val instance = ProtoUtils.getInstance(res.asJava)
                  requestBuilder.addInstances(instance)
                case jInt: JDouble =>
                  var example: Any = null
                  valueType match {
                    case TypesProtos.DataType.DT_DOUBLE =>
                      example = jInt.values
                    case _ => new Exception("unsuported data type!")
                  }
                  val res = Map[String, Any](key -> example)
                  val instance = ProtoUtils.getInstance(res.asJava)
                  requestBuilder.addInstances(instance)
                case jString: JString =>
                  var example: Any = null
                  valueType match {
                    case TypesProtos.DataType.DT_STRING =>
                      example = jString.values
                    case _ => new Exception("unsuported data type!")
                  }
                  val res = Map[String, Any](key -> example)
                  val instance = ProtoUtils.getInstance(res.asJava)
                  requestBuilder.addInstances(instance)
                case jBool: JBool =>
                  var example: Any = null
                  valueType match {
                    case TypesProtos.DataType.DT_BOOL =>
                      example = jBool.values
                    case _ => new Exception("unsuported data type!")
                  }
                  val res = Map[String, Any](key -> example)
                  val instance = ProtoUtils.getInstance(res.asJava)
                  requestBuilder.addInstances(instance)
                case _ =>
                  instanceName = values.toString
                  val instance = ProtoUtils.getInstance(instanceName, examples.asJava)
                  requestBuilder.addInstances(instance)
              }
            }
          } else {
            var example: Map[String, Any] = null
            valueType match {
              case TypesProtos.DataType.DT_DOUBLE =>
                example = ele.asInstanceOf[JObject].values.map {
                  case (k, v: Double) => (k, v)
                  case (k, v: Float) => (k, v.toDouble)
                  case (k, v: Long) => (k, v.toDouble)
                  case (k, v: Int) => (k, v.toDouble)
                  case (k, v: String) => (k, v.toDouble)
                  case (k, v: BigInt) => (k, v.toDouble)
                  case (_, v) =>
                    throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                }
              case TypesProtos.DataType.DT_INT32 =>
                example = ele.asInstanceOf[JObject].values.map {
                  case (k, v: BigInt) => (k, v.toInt)
                  case (k, v: Int) => (k, v)
                  case (k, v: String) => (k, v.toInt)
                  case (_, v) =>
                    throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Int")
                }
              case TypesProtos.DataType.DT_STRING =>
                example = ele.asInstanceOf[JObject].values.map {
                  case (k, v: String) => (k, v)
                  case (_, v) =>
                    throw new Exception(s"${v.getClass.getSimpleName} Data Type not match String")
                }
              case _ => new Exception("unsuported data type!")
            }
            val instance = ProtoUtils.getInstance(example.asJava)
            requestBuilder.addInstances(instance)
          }
        } else if (dim < 0) {
          //pmml prediction data parse
          if (ele.asInstanceOf[JObject].values.size <= 2) {
            var instanceName: String = ""
            var example: Map[String, Any] = null
            for ((_, values) <- ele.asInstanceOf[JObject].obj) {
              values match {
                case jObject: JObject =>
                  valueType match {
                    case TypesProtos.DataType.DT_STRING =>
                      example = jObject.values.map {
                        case (k, v: String) => (k, v)
                        case (_, v) =>
                          throw new Exception(s"${v.getClass.getSimpleName} Data Type not match String")
                      }
                    case TypesProtos.DataType.DT_INT32 =>
                      example = jObject.values.map {
                        case (k, v: Int) => (k, v)
                        case (k, v: String) => (k, v.toInt)
                        case (k, v: BigInt) => (k, v.toInt)
                        case (_, v) =>
                          throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Int")
                      }
                    case TypesProtos.DataType.DT_INT64 =>
                      example = jObject.values.map {
                        case (k, v: Long) => (k, v)
                        case (k, v: Int) => (k, v.toLong)
                        case (k, v: String) => (k, v.toLong)
                        case (k, v: BigInt) => (k, v.toLong)
                        case (_, v) =>
                          throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Long")
                      }
                    case TypesProtos.DataType.DT_FLOAT =>
                      example = jObject.values.map {
                        case (k, v: Double) => (k, v.toFloat)
                        case (k, v: Float) => (k, v)
                        case (k, v: Long) => (k, v.toFloat)
                        case (k, v: Int) => (k, v.toFloat)
                        case (k, v: String) => (k, v.toFloat)
                        case (k, v: BigInt) => (k, v.toFloat)
                        case (_, v) =>
                          throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Float")
                      }
                    case TypesProtos.DataType.DT_DOUBLE =>
                      example = jObject.values.map {
                        case (k, v: Double) => (k, v)
                        case (k, v: Float) => (k, v.toDouble)
                        case (k, v: Long) => (k, v.toDouble)
                        case (k, v: Int) => (k, v.toDouble)
                        case (k, v: String) => (k, v.toDouble)
                        case (k, v: BigInt) => (k, v.toDouble)
                        case (_, v) =>
                          throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                      }
                    case _ => new Exception("unsuported data type!")
                  }
                case _ =>
                  instanceName = values.toString
              }
            }
            val instance = ProtoUtils.getInstance(instanceName, example.asJava)
            requestBuilder.addInstances(instance)
          } else {
            var example: Map[String, Any] = null
            valueType match {
              case TypesProtos.DataType.DT_STRING =>
                example = ele.asInstanceOf[JObject].values.map {
                  case (k, v: String) => (k, v)
                  case (_, v) =>
                    throw new Exception(s"${v.getClass.getSimpleName} Data Type not match String")
                }
              case TypesProtos.DataType.DT_INT32 =>
                example = ele.asInstanceOf[JObject].values.map {
                  case (k, v: BigInt) => (k, v.toInt)
                  case (k, v: Int) => (k, v)
                  case (k, v: String) => (k, v.toInt)
                  case (_, v) =>
                    throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Int")
                }
              case TypesProtos.DataType.DT_INT64 =>
                example = ele.asInstanceOf[JObject].values.map {
                  case (k, v: BigInt) => (k, v.toLong)
                  case (k, v: Long) => (k, v)
                  case (k, v: Int) => (k, v.toLong)
                  case (k, v: String) => (k, v.toLong)
                  case (_, v) =>
                    throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Long")
                }
              case TypesProtos.DataType.DT_FLOAT =>
                example = ele.asInstanceOf[JObject].values.map {
                  case (k, v: Double) => (k, v.toFloat)
                  case (k, v: Float) => (k, v)
                  case (k, v: Long) => (k, v.toFloat)
                  case (k, v: Int) => (k, v.toFloat)
                  case (k, v: String) => (k, v.toFloat)
                  case (k, v: BigInt) => (k, v.toFloat)
                  case (_, v) =>
                    throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Float")
                }
              case TypesProtos.DataType.DT_DOUBLE =>
                example = ele.asInstanceOf[JObject].values.map {
                  case (k, v: Double) => (k, v)
                  case (k, v: Float) => (k, v.toDouble)
                  case (k, v: Long) => (k, v.toDouble)
                  case (k, v: Int) => (k, v.toDouble)
                  case (k, v: String) => (k, v.toDouble)
                  case (k, v: BigInt) => (k, v.toDouble)
                  case (_, v) =>
                    throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                }
              case _ => new Exception("unsuported data type!")
            }
            val instance = ProtoUtils.getInstance(example.asJava)
            requestBuilder.addInstances(instance)
          }
        } else {
          //angel prediction data parse
          ele.asInstanceOf[JObject] \ Json2Instances.sparseKey match {
            case JNothing =>
              if (ele.asInstanceOf[JObject].values.size <= 2) {
                var instanceName: String = ""
                for ((key, values) <- ele.asInstanceOf[JObject].obj) {
                  values match {
                    case array: JArray =>
                      var example: List[Any] = null
                      valueType match {
                        case TypesProtos.DataType.DT_STRING =>
                          example = array.values.map {
                            case x: String => x
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match String")
                          }
                        case TypesProtos.DataType.DT_INT32 =>
                          example = array.values.map {
                            case v: BigInt => v.toInt
                            case v: Int => v
                            case v: String => v.toInt
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Int")
                          }
                        case TypesProtos.DataType.DT_INT64 =>
                          example = array.values.map {
                            case v: BigInt => v.toLong
                            case v: Long => v
                            case v: Int => v.toLong
                            case v: String => v.toLong
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Long")
                          }
                        case TypesProtos.DataType.DT_FLOAT =>
                          example = array.values.map {
                            case v: Double => v.toFloat
                            case v: Float => v
                            case v: Long => v.toFloat
                            case v: Int => v.toFloat
                            case v: String => v.toFloat
                            case v: BigInt => v.toFloat
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Float")
                          }
                        case TypesProtos.DataType.DT_DOUBLE =>
                          example = array.values.map {
                            case v: Double => v
                            case v: Float => v.toDouble
                            case v: Long => v.toDouble
                            case v: Int => v.toDouble
                            case v: String => v.toDouble
                            case v: BigInt => v.toDouble
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                          }
                        case _ => new Exception("unsuported data type!")
                      }
                      val instance = ProtoUtils.getInstance(instanceName, example.iterator.asJava)
                      requestBuilder.addInstances(instance)
                    case jObject: JObject =>
                      var example: Map[String, Any] = null
                      valueType match {
                        case TypesProtos.DataType.DT_STRING =>
                          example = jObject.values.map {
                            case (k, v: String) => (k, v)
                            case (_, v) =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match String")
                          }
                        case TypesProtos.DataType.DT_INT32 =>
                          example = jObject.values.map {
                            case (k, v: BigInt) => (k, v.toInt)
                            case (k, v: Int) => (k, v)
                            case (k, v: String) => (k, v.toInt)
                            case (_, v) =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Int")
                          }
                        case TypesProtos.DataType.DT_INT64 =>
                          example = jObject.values.map {
                            case (k, v: BigInt) => (k, v.toLong)
                            case (k, v: Long) => (k, v)
                            case (k, v: Int) => (k, v.toLong)
                            case (k, v: String) => (k, v.toLong)
                            case (_, v) =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Long")
                          }
                        case TypesProtos.DataType.DT_FLOAT =>
                          example = jObject.values.map {
                            case (k, v: Double) => (k, v.toFloat)
                            case (k, v: Float) => (k, v)
                            case (k, v: Long) => (k, v.toFloat)
                            case (k, v: Int) => (k, v.toFloat)
                            case (k, v: String) => (k, v.toFloat)
                            case (k, v: BigInt) => (k, v.toFloat)
                            case (_, v) =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Float")
                          }
                        case TypesProtos.DataType.DT_DOUBLE =>
                          example = jObject.values.map {
                            case (k, v: Double) => (k, v)
                            case (k, v: Float) => (k, v.toDouble)
                            case (k, v: Long) => (k, v.toDouble)
                            case (k, v: Int) => (k, v.toDouble)
                            case (k, v: String) => (k, v.toDouble)
                            case (k, v: BigInt) => (k, v.toDouble)
                            case (_, v) =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                          }
                        case _ => new Exception("unsuported data type!")
                      }
                      val instance = ProtoUtils.getInstance(instanceName, example.asJava)
                      requestBuilder.addInstances(instance)
                    case _ =>
                      instanceName = values.toString
                  }
                }

              } else {
                var example: Map[String, Any] = null
                //val example = ele.asInstanceOf[JObject].values
                valueType match {
                  case TypesProtos.DataType.DT_STRING =>
                    example = ele.asInstanceOf[JObject].values.map {
                      case (k, v: String) => (k, v)
                      case (_, v) =>
                        throw new Exception(s"${v.getClass.getSimpleName} Data Type not match String")
                    }
                  case TypesProtos.DataType.DT_INT32 =>
                    example = ele.asInstanceOf[JObject].values.map {
                      case (k, v: BigInt) => (k, v.toInt)
                      case (k, v: Int) => (k, v)
                      case (k, v: String) => (k, v.toInt)
                      case (_, v) =>
                        throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Int")
                    }
                  case TypesProtos.DataType.DT_INT64 =>
                    example = ele.asInstanceOf[JObject].values.map {
                      case (k, v: BigInt) => (k, v.toLong)
                      case (k, v: Long) => (k, v)
                      case (k, v: Int) => (k, v.toLong)
                      case (k, v: String) => (k, v.toLong)
                      case (_, v) =>
                        throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Long")
                    }
                  case TypesProtos.DataType.DT_FLOAT =>
                    example = ele.asInstanceOf[JObject].values.map {
                      case (k, v: Double) => (k, v.toFloat)
                      case (k, v: Float) => (k, v)
                      case (k, v: Long) => (k, v.toFloat)
                      case (k, v: Int) => (k, v.toFloat)
                      case (k, v: String) => (k, v.toFloat)
                      case (k, v: BigInt) => (k, v.toFloat)
                      case (_, v) =>
                        throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Float")
                    }
                  case TypesProtos.DataType.DT_DOUBLE =>
                    example = ele.asInstanceOf[JObject].values.map {
                      case (k, v: Double) => (k, v)
                      case (k, v: Float) => (k, v.toDouble)
                      case (k, v: Long) => (k, v.toDouble)
                      case (k, v: Int) => (k, v.toDouble)
                      case (k, v: String) => (k, v.toDouble)
                      case (k, v: BigInt) => (k, v.toDouble)
                      case (_, v) =>
                        throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                    }
                  case _ => new Exception("unsuported data type!")
                }
                val instance = ProtoUtils.getInstance(example.asJava)
                requestBuilder.addInstances(instance)
              }
            case arrIndices: JArray =>
              ele.asInstanceOf[JObject] \ Json2Instances.sparseValue match {
                case arrValues: JArray =>
                  keyType match {
                    case TypesProtos.DataType.DT_INT32 =>
                      val indices = arrIndices.values.map(x => x.asInstanceOf[BigInt].toInt.asInstanceOf[java.lang.Integer])
                      var example: List[Any] = null
                      valueType match {
                        case TypesProtos.DataType.DT_STRING =>
                          example = arrValues.values.map {
                            case v: String => v
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match String")
                          }
                        case TypesProtos.DataType.DT_INT32 =>
                          example = arrValues.values.map {
                            case v: BigInt => v.toInt
                            case v: Int => v
                            case v: String => v.toInt
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Int")
                          }
                        case TypesProtos.DataType.DT_INT64 =>
                          example = arrValues.values.map {
                            case v: BigInt => v.toLong
                            case v: Long => v
                            case v: Int => v.toLong
                            case v: String => v.toLong
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Long")
                          }
                        case TypesProtos.DataType.DT_FLOAT =>
                          example = arrValues.values.map {
                            case v: Float => v
                            case v: Double => v.toFloat
                            case v: Long => v.toFloat
                            case v: Int => v.toFloat
                            case v: String => v.toFloat
                            case v: BigInt => v.toFloat
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Float")
                          }
                        case TypesProtos.DataType.DT_DOUBLE =>
                          example = arrValues.values.map {
                            case v: Double => v
                            case v: Float => v.toDouble
                            case v: Long => v.toDouble
                            case v: Int => v.toDouble
                            case v: String => v.toDouble
                            case v: BigInt => v.toDouble
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                          }
                        case _ => new Exception("unsuported data type!")
                      }
                      val instance = ProtoUtils.getInstance(dim.asInstanceOf[Int], indices.zip(example).toMap.asJava)
                      requestBuilder.addInstances(instance)
                    case TypesProtos.DataType.DT_INT64 =>
                      val indices = arrIndices.values.map(x => x.asInstanceOf[BigInt].toLong.asInstanceOf[java.lang.Long])
                      var example: List[Any] = null
                      valueType match {
                        case TypesProtos.DataType.DT_STRING =>
                          example = arrValues.values.map {
                            case v: String => v
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match String")
                          }
                        case TypesProtos.DataType.DT_INT32 =>
                          example = arrValues.values.map {
                            case v: BigInt => v.toInt
                            case v: Int => v
                            case x: String => x.toInt
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Int")
                          }
                        case TypesProtos.DataType.DT_INT64 =>
                          example = arrValues.values.map {
                            case v: BigInt => v.toLong
                            case v: Long => v
                            case v: Int => v.toLong
                            case x: String => x.toLong
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Long")
                          }
                        case TypesProtos.DataType.DT_FLOAT =>
                          example = arrValues.values.map {
                            case v: Double => v.toFloat
                            case v: Float => v
                            case v: Long => v.toFloat
                            case v: Int => v.toFloat
                            case x: String => x.toFloat
                            case v: BigInt => v.toFloat
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Float")
                          }
                        case TypesProtos.DataType.DT_DOUBLE =>
                          example = arrValues.values.map {
                            case v: Double => v
                            case v: Float => v.toDouble
                            case v: Long => v.toDouble
                            case v: Int => v.toDouble
                            case x: String => x.toDouble
                            case v: BigInt => v.toDouble
                            case v =>
                              throw new Exception(s"${v.getClass.getSimpleName} Data Type not match Double")
                          }
                        case _ => new Exception("unsuported data type!")
                      }
                      val instance = ProtoUtils.getInstance(dim.asInstanceOf[Long], indices.zip(example).toMap.asJava)
                      requestBuilder.addInstances(instance)
                    case _ => new Exception("unsuported key data type!")
                  }
                case _ => throw new Exception("Json format error!")
              }
            case _ => throw new Exception("Json format error!")
          }
        }
      } else {
        //angel prediction data parse
        if (isElementObject(ele)) {
          throw new Exception("Expecting value/list but got object at item " + instanceCount + " of input list")
        }
        ele match {
          case array: JArray =>
            var example: List[Any] = null
            valueType match {
              case TypesProtos.DataType.DT_STRING =>
                example = array.values.map {
                  case x: String => x
                  case x =>
                    throw new Exception(s"${x.getClass.getSimpleName} Data Type not match String")
                }
              case TypesProtos.DataType.DT_INT32 =>
                example = array.values.map {
                  case x: BigInt => x.toInt
                  case x: Int => x
                  case x: String => x.toInt
                  case x =>
                    throw new Exception(s"${x.getClass.getSimpleName} Data Type not match BigInt")
                }
              case TypesProtos.DataType.DT_INT64 =>
                example = array.values.map {
                  case x: BigInt => x.toLong
                  case x: Long => x
                  case x: Int => x.toLong
                  case x: String => x.toLong
                  case x =>
                    throw new Exception(s"${x.getClass.getSimpleName} Data Type not match BigInt")
                }
              case TypesProtos.DataType.DT_FLOAT =>
                example = array.values.map {
                  case x: Float => x
                  case x: Double => x.toFloat
                  case x: Long => x.toFloat
                  case x: Int => x.toInt
                  case x: String => x.toFloat
                  case v: BigInt => v.toFloat
                  case x =>
                    throw new Exception(s"${x.getClass.getSimpleName} Data Type not match Float")
                }
              case TypesProtos.DataType.DT_DOUBLE =>
                example = array.values.map {
                  case x: Double => x
                  case x: Float => x.toDouble
                  case x: Long => x.toDouble
                  case x: Int => x.toDouble
                  case x: String => x.toDouble
                  case v: BigInt => v.toDouble
                  case x =>
                    throw new Exception(s"${x.getClass.getSimpleName} Data Type not match Double")
                }
              case _ => new Exception("unsuported data type!")
            }
            val instance = ProtoUtils.getInstance(example.iterator.asJava)
            requestBuilder.addInstances(instance)
          case str: JString =>
            val instance = ProtoUtils.getInstance(str.values)
            requestBuilder.addInstances(instance)
          case _ => new Exception("Json format error!")
        }
      }
      instanceCount = instanceCount + 1
    }
  }

  def isValBase64Object(value: Object): Boolean = {
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


