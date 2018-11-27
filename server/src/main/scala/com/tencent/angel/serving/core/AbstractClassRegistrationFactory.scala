package com.tencent.angel.serving.core


import com.google.protobuf.{Descriptors, Message}
import com.tencent.angel.config.PlatformConfigProtos

import scala.collection.mutable

class AbstractClassRegistrationFactory[BaseClass, AdditionalFactoryArgs] {

  def create(config: Message, args: Array[AdditionalFactoryArgs]): BaseClass = ???

}

//template <typename BaseClass, typename Class, typename Config, typename... AdditionalFactoryArgs>
class ClassRegistrationFactory[BaseClass, ClassCreator, Config, AdditionalFactoryArgs]
  extends AbstractClassRegistrationFactory [BaseClass, AdditionalFactoryArgs]{
  private var configDescriptor: Descriptors.Descriptor = null


  override def create(config: Message, args: Array[AdditionalFactoryArgs]): BaseClass = {
    if (config.getDescriptorForType.getFullName != configDescriptor.getFullName){
      throw InvalidArguments(s"Supplied config proto of type ${config.getDescriptorForType.getFullName} does not match " +
        s"expected type ${configDescriptor.getFullName}")
    }
    ???
  }
}

class ClassRegistry[RegistryName, BaseClass, AdditionalFactoryArgs]{
  type FactoryType = AbstractClassRegistrationFactory[BaseClass, AdditionalFactoryArgs]

  //create an instance of BaseClass based on a config proto
  def create(config: Message, args: Array[AdditionalFactoryArgs]): BaseClass = {
    val configProtoMessageType = config.getDescriptorForType.getFullName
    val factory = lookupFromMap(configProtoMessageType)
    if (factory == null){
      throw InvalidArguments(s"Couldn't find factory for config proto message type ${configProtoMessageType}")
    }
    factory.create(config,args)
  }

  def createFromAny(anyConfig: com.google.protobuf.Any, args: Array[AdditionalFactoryArgs]): BaseClass = {
    val fullTypeName = parseUrlForAnyType(anyConfig.getTypeUrl)
    val descriptor:Descriptors.Descriptor = PlatformConfigProtos.getDescriptor.findMessageTypeByName(fullTypeName)
    val config: Message = descriptor.getOptions.newBuilderForType().build()
    create(config, args)
  }

  def lookupFromMap(configProtoMessageType: String): FactoryType ={
    val globalMap = globalFactoryMap()
    //floableMap lock
    try{
      globalMap.factoryMap.get(configProtoMessageType).getOrElse(null)
    }finally {

    }
  }

  def globalFactoryMap(): LockedFactoryMap = {
    LockedFactoryMap(mutable.Map[String, FactoryType]())
  }

  case class LockedFactoryMap(factoryMap: mutable.Map[String, FactoryType])


  def parseUrlForAnyType(typeUrl: String): String ={
    typeUrl.substring(typeUrl.lastIndexOf("/"))
  }
}