package com.tencent.angel.serving.core


import java.util.concurrent.locks.ReentrantReadWriteLock
import com.google.protobuf.{Descriptors, Message}
import com.tencent.angel.config.PlatformConfigProtos

import scala.reflect.runtime.{universe => ru}


object ClassRegistrationFactory{

  def create[BaseClass](config: Message, classCreator: String, methodName: String): BaseClass = {
    val classMirror = ru.runtimeMirror(getClass.getClassLoader)
    val classInstance = classMirror.staticModule(classCreator)
    val methods = classMirror.reflectModule(classInstance)
    val objMirror = classMirror.reflect(methods.instance)
    val method = methods.symbol.typeSignature.member(ru.TermName(methodName)).asMethod
    val result = objMirror.reflectMethod(method)(config)
    result.asInstanceOf[BaseClass]
  }

}

object ClassRegistry {

  private val globalMapLock = new ReentrantReadWriteLock()
  private val globalMapReadLock: ReentrantReadWriteLock.ReadLock = globalMapLock.readLock()
  private val globalMapWriteLock: ReentrantReadWriteLock.WriteLock = globalMapLock.writeLock()

  //create an instance of BaseClass based on a config proto
  def create[BaseClass](config: Message, classCreator:String): BaseClass = {
    ClassRegistrationFactory.create[BaseClass](config, classCreator, "create")
  }

  def createFromAny[BaseClass](anyConfig: com.google.protobuf.Any): BaseClass = {
    val fullTypeName = parseUrlForAnyType(anyConfig.getTypeUrl)
    val descriptor: Descriptors.Descriptor = PlatformConfigProtos.getDescriptor.findMessageTypeByName(fullTypeName)
    val config: Message = descriptor.getOptions.newBuilderForType().build()
    create[BaseClass](config, fullTypeName+"Creator")
  }


  private def parseUrlForAnyType(typeUrl: String): String ={
    typeUrl.substring(typeUrl.lastIndexOf("/"))
  }
}