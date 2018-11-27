package com.tencent.angel.config

import com.tencent.angel.config.ModelServerConfigProtos.ModelServerConfig.ConfigCase
import com.tencent.angel.config.SamplingConfigProtos.LoggingConfig

case class ModelConfig(name: String, basePath: String, modelPlatform: String,
                  modelVersionPolicy: ServableVersionPolicy,
                  loggingConfig: LoggingConfig){

  def toProto: ModelServerConfigProtos.ModelConfig = {
    val builder = ModelServerConfigProtos.ModelConfig.newBuilder()
    builder.setName(name)
    builder.setBasePath(basePath)
    builder.setModelPlatform(modelPlatform)
    builder.setModelVersionPolicy(modelVersionPolicy.toProto)
    builder.setLoggingConfig(loggingConfig)
    builder.build()
  }

  override def toString: String = toProto.toString

  override def equals(obj: scala.Any): Boolean = {
    val other = obj.asInstanceOf[ModelConfig]
    this.name == other.name && this.basePath == other.basePath && this.modelPlatform == other.modelPlatform &&
    this.modelVersionPolicy == this.modelVersionPolicy && this.loggingConfig == other.loggingConfig
  }

  override def hashCode(): Int = {
    name.hashCode + basePath.hashCode + modelPlatform.hashCode + modelVersionPolicy.hashCode + loggingConfig.hashCode()
  }

  def ==(other: ModelConfig): Boolean = this.equals(other)

  def !=(other: ModelConfig): Boolean = !this.equals(other)
}

object ModelConfig{

  def apply(modelConfig: ModelServerConfigProtos.ModelConfig): ModelConfig = {
    ModelConfig(modelConfig.getName, modelConfig.getBasePath, modelConfig.getModelPlatform,
      ServableVersionPolicy(modelConfig.getModelVersionPolicy), modelConfig.getLoggingConfig)
  }

  def apply(modelConfig: String): ModelConfig = {
    val modelConfigProto = ModelServerConfigProtos.ModelConfig.parseFrom(modelConfig.getBytes)
    ModelConfig(modelConfigProto.getName, modelConfigProto.getBasePath, modelConfigProto.getModelPlatform,
      ServableVersionPolicy(modelConfigProto.getModelVersionPolicy), modelConfigProto.getLoggingConfig)
  }
}

case class ModelConfigList(val modelConfigs: List[ModelConfig]){
  def toProto: ModelServerConfigProtos.ModelConfigList ={
    val builder = ModelServerConfigProtos.ModelConfigList.newBuilder()
    modelConfigs.foreach( modelConfig =>
      builder.addConfig(modelConfig.toProto)
    )
    builder.build()
  }

  override def toString: String = toProto.toString

  override def equals(obj: scala.Any): Boolean = this.equals(obj)

  def ==(other: ModelConfigList): Boolean = {
    this.modelConfigs.zip(other.modelConfigs).forall { case (thisModelConfigList, otherModelConfigList) =>
      thisModelConfigList == otherModelConfigList
    }
  }

  def !=(other: ModelConfigList): Boolean = !(this == other)

}

object ModelConfigList{

  def apply(modelConfigList: ModelServerConfigProtos.ModelConfigList): ModelConfigList = {
    val modelConfigs = (0 until modelConfigList.getConfigCount).toList.map { idx =>
      ModelConfig(modelConfigList.getConfig(idx))
    }
    ModelConfigList(modelConfigs)
  }

  def apply(modelConfigList: String): ModelConfigList = {
    val modelConfigProto = ModelServerConfigProtos.ModelConfigList.parseFrom(modelConfigList.getBytes)
    val modelConfigs = (0 until modelConfigProto.getConfigCount).toList.map{ idx =>
      ModelConfig(modelConfigProto.getConfig(idx))
    }
    ModelConfigList(modelConfigs)
  }

}

sealed trait ModelServerConfig

case class ModelServerConfigList(modelConfigList: ModelConfigList) extends ModelServerConfig {
  def toProto: ModelServerConfigProtos.ModelServerConfig = {
    val builder = ModelServerConfigProtos.ModelServerConfig.newBuilder()
    builder.setModelConfigList(modelConfigList.toProto)
    builder.build()
  }

  override def toString: String = toProto.toString

  override def equals(obj: scala.Any): Boolean = this.equals(obj)

  def ==(other: ModelServerConfigList): Boolean = this.equals(other)

  def !=(other: ModelServerConfigList): Boolean = !(this == other)
}

case class CustomModelConfig(customModelConfig: com.google.protobuf.Any) extends ModelServerConfig

object ModelServerConfig{
  def apply(modelConfigList: ModelConfigList): ModelServerConfig = ModelServerConfigList(modelConfigList)
  def apply(customModelConfig: com.google.protobuf.Any): ModelServerConfig = CustomModelConfig(customModelConfig)

  def apply(modelServerConfig: ModelServerConfigProtos.ModelServerConfig): ModelServerConfig = {
    modelServerConfig.getConfigCase match {
      case ConfigCase.MODEL_CONFIG_LIST =>
        val modelConfigList = ModelConfigList(modelServerConfig.getModelConfigList)
        ModelServerConfigList(modelConfigList)
      case ConfigCase.CUSTOM_MODEL_CONFIG =>
        val customModelConfig = modelServerConfig.getCustomModelConfig
        CustomModelConfig(customModelConfig)
      case ConfigCase.CONFIG_NOT_SET =>
        throw new Exception("")
    }
  }

  def apply(modelServerConfig: String): ModelServerConfig = {
    val modelServerConfigProto = ModelServerConfigProtos.ModelServerConfig.parseFrom(modelServerConfig.getBytes)
    modelServerConfigProto.getConfigCase match {
      case ConfigCase.MODEL_CONFIG_LIST =>
        val modelConfigList = ModelConfigList(modelServerConfigProto.getModelConfigList)
        ModelServerConfigList(modelConfigList)
      case ConfigCase.CUSTOM_MODEL_CONFIG =>
        val customModelConfig = modelServerConfigProto.getCustomModelConfig
        CustomModelConfig(customModelConfig)
      case ConfigCase.CONFIG_NOT_SET =>
        throw new Exception("")
    }
  }
}
