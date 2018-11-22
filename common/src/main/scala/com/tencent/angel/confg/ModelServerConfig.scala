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


class ModelServerConfig(){

}

object ModelServerConfig{
  def apply(modelConfigList: ModelConfigList): ModelServerConfig = new ModelServerConfig()

  def apply(modelServerConfig: ModelServerConfig): ModelServerConfig = ???

  def apply(modelServerConfig: String): ModelServerConfig = ???

  def apply(customModelConfig: com.google.protobuf.Any): ModelServerConfig = new ModelServerConfig()

  def apply(customModelConfig: com.google.protobuf.Any): ModelServerConfig = ???
}
