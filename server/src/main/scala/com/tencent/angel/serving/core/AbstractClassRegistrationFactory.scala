package com.tencent.angel.serving.core


import java.util.concurrent.locks.ReentrantReadWriteLock
import com.google.protobuf.{Descriptors, Message}
import com.tencent.angel.config.PlatformConfigProtos

import scala.collection.mutable

class AbstractClassRegistrationFactory[BaseClass] {

  def create(config: Message): BaseClass = ???

}

//template <typename BaseClass, typename Class, typename Config, typename... AdditionalFactoryArgs>
class ClassRegistrationFactory[BaseClass]
  extends AbstractClassRegistrationFactory [BaseClass]{
  private var configDescriptor: Descriptors.Descriptor = null
  private var classCreator: ClassCreator[BaseClass] = null

  def this(classCreator: ClassCreator[BaseClass],configDescriptor: Descriptors.Descriptor){
    this()
    this.classCreator = classCreator
    this.configDescriptor = configDescriptor
  }

  override def create(config: Message): BaseClass = {
    if (config.getDescriptorForType.getFullName != configDescriptor.getFullName){
      throw InvalidArguments(s"Supplied config proto of type ${config.getDescriptorForType.getFullName} does not match " +
        s"expected type ${configDescriptor.getFullName}")
    }
    classCreator.create(config)
  }
}


object ClassRegistry {
  type FactoryType = AbstractClassRegistrationFactory[_]

  private val factoryMap: mutable.Map[String, FactoryType] = mutable.Map[String, FactoryType]()
  private val globalMapLock = new ReentrantReadWriteLock()
  private val globalMapReadLock: ReentrantReadWriteLock.ReadLock = globalMapLock.readLock()
  private val globalMapWriteLock: ReentrantReadWriteLock.WriteLock = globalMapLock.writeLock()

  //create an instance of BaseClass based on a config proto
  def create[BaseClass](config: Message): BaseClass = {
    val configProtoMessageType = config.getDescriptorForType.getFullName
    val factory = lookupFromMap(configProtoMessageType)
    if (factory == null){
      throw InvalidArguments(s"Couldn't find factory for config proto message type ${configProtoMessageType}")
    }
    factory.create(config).asInstanceOf[BaseClass]
  }

  def createFromAny[BaseClass](anyConfig: com.google.protobuf.Any): BaseClass = {
    val fullTypeName = parseUrlForAnyType(anyConfig.getTypeUrl)
    val descriptor:Descriptors.Descriptor = PlatformConfigProtos.getDescriptor.findMessageTypeByName(fullTypeName)
    val config: Message = descriptor.getOptions.newBuilderForType().build()
    create[BaseClass](config)
  }

  def register[BaseClass](classCreator: ClassCreator[BaseClass],configDescriptor: Descriptors.Descriptor): Unit ={
    insertIntoMap(configDescriptor.getFullName, new ClassRegistrationFactory[BaseClass](classCreator, configDescriptor))
  }

  private def lookupFromMap(configProtoMessageType: String): FactoryType ={
    globalMapReadLock.lock()
    try{
      factoryMap.getOrElse(configProtoMessageType, null)
    }finally {
      globalMapReadLock.unlock()
    }
  }

  private def insertIntoMap(configProtoMessageType: String, factory: FactoryType): Unit ={
    globalMapWriteLock.lock()
    try{
      factoryMap.put(configProtoMessageType, factory)
    }finally {
      globalMapWriteLock.unlock()
    }
  }

  def globalFactoryMap(): mutable.Map[String, FactoryType]= factoryMap

  private def parseUrlForAnyType(typeUrl: String): String ={
    typeUrl.substring(typeUrl.lastIndexOf("/"))
  }
}