package com.tencent.angel.serving.core

import scala.collection.mutable

object DeviceType{
  val kMain, kGpu: String = ""
}

case class ResourceOptions(devices: mutable.Map[String, Int])

class ResourceUtil(options: ResourceOptions) {



}
