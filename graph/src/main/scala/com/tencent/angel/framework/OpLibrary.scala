package com.tencent.angel.framework

import java.io.{FileInputStream, InputStream}

import com.tencent.angel.core.gen.OpDefProtos

class OpLibrary(libFile: String) {
  val opDefMap : Map[String, OpDefProtos.OpDef] = init()

  def getOpDef(name: String): OpDef = {
    opDefMap.getOrElse(name, null.asInstanceOf[OpDefProtos.OpDef])
  }

  private def init(): Map[String, OpDefProtos.OpDef] = {
    val libFullName = this.getClass.getClassLoader.getResource(libFile).getPath
    val is: InputStream = new FileInputStream(libFullName)
    val opList = OpDefProtos.OpList.parseFrom(is)
    (0 until opList.getOpCount).map{i =>
      val opDef = opList.getOp(i)
      opDef.getName -> opDef
    }.toMap
  }

}
