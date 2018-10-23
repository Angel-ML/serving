package com.tencent.angel.framework

import java.util

case class LocationTag(name:String, functionName:String, fileName:String, lineNumber:Int)

class Registry(val name: String) {
  private val registry: util.Map[String, LocationTag] = new util.HashMap[String, LocationTag]()

  def register(candicate: Any, name:String = ""): Unit = {

  }

  def lookUp(name:String): Unit = ???

}
