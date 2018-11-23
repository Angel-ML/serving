package com.tencent.angel.confg

import com.tencent.angel.config.ModelServerConfigProtos
import com.tencent.angel.config.ModelServerConfigProtos.ModelServerConfig.ConfigCase

class ModelConfig{

}

object ModelConfig{

}

class ModelConfigList{

}

object ModelConfigList{

}


sealed trait ModelServerConfig

class ModelServerConfigList extends ModelServerConfig

class ModelServerConfigCustom extends ModelServerConfig


object ModelServerConfig{
  def apply(modelConfigList: ModelConfigList): ModelServerConfig = {

  }

  def apply(modelServerConfig: ModelServerConfigProtos.ModelServerConfig): ModelServerConfig = {
    modelServerConfig.getConfigCase match {
      case ModelServerConfigProtos.ModelServerConfig.ConfigCase.CONFIG_NOT_SET =>
        throw ...
      case ModelServerConfigProtos.ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST =>
        new ModelServerConfigList
      case ModelServerConfigProtos.ModelServerConfig.ConfigCase.CUSTOM_MODEL_CONFIG =>
        new ModelServerConfigCustom
    }
  }

  def apply(modelServerConfig: String): ModelServerConfig = ???

  def apply(customModelConfig: com.google.protobuf.Any): ModelServerConfig = new ModelServerConfig()

  def apply(customModelConfig: com.google.protobuf.Any): ModelServerConfig = ???
}
