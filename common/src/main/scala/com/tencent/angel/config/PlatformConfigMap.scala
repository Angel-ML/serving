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
package com.tencent.angel.config

import java.util
import java.util.Map

import com.tencent.angel.config.PlatformConfigProtos

import scala.collection.mutable

case class PlatformConfig(sourceAdapterConfig: com.google.protobuf.Any) {
  def toProto: PlatformConfigProtos.PlatformConfig = {
    val builder = PlatformConfigProtos.PlatformConfig.newBuilder()
    builder.setSourceAdapterConfig(sourceAdapterConfig)
    builder.build()
  }

  override def toString: String = toProto.toString

  override def equals(obj: scala.Any): Boolean = this.equals(obj)

  def ==(other: PlatformConfig): Boolean = this.sourceAdapterConfig == other.sourceAdapterConfig

  def !=(other: PlatformConfig): Boolean = !(this == other)
}

object PlatformConfig {

  def apply(platformConfig: PlatformConfigProtos.PlatformConfig): PlatformConfig = {
    val sourceAdapterConfig = platformConfig.getSourceAdapterConfig
    PlatformConfig(sourceAdapterConfig)
  }

  def apply(sourceAdapterConfig: String): PlatformConfig = {
    val platformConfigProtos = PlatformConfigProtos.PlatformConfig.parseFrom(sourceAdapterConfig.getBytes)
    PlatformConfig(platformConfigProtos.getSourceAdapterConfig)
  }
}


case class PlatformConfigMap(platformConfigMap: mutable.HashMap[String, PlatformConfig]) {
  def toProto: PlatformConfigProtos.PlatformConfigMap = {
    val builder = PlatformConfigProtos.PlatformConfigMap.newBuilder()
    val platformMap = new util.HashMap[String, PlatformConfigProtos.PlatformConfig]()
    platformConfigMap.foreach{case (platform, platformConfig) =>
      platformMap.put(platform, platformConfig.toProto)
    }
    builder.putAllPlatformConfigs(platformMap)
    builder.build()
  }

  override def toString: String = toProto.toString

  override def equals(obj: scala.Any): Boolean = this.equals(obj)

  def ==(other: PlatformConfigMap): Boolean = {
    this.platformConfigMap.zip(other.platformConfigMap).forall { case (thisPlatformMap, otherPlatformMap) =>
      thisPlatformMap == otherPlatformMap
    }
  }

  def !=(other: PlatformConfigMap): Boolean = !(this == other)
}

object PlatformConfigMap {

  def apply(platformConfigMap: PlatformConfigProtos.PlatformConfigMap): PlatformConfigMap = {
    val platformMap = mutable.HashMap[String, PlatformConfig]()
    val it = platformConfigMap.getPlatformConfigsMap.entrySet().iterator()
    while (it.hasNext){
      val platform = it.next()
      platformMap += (platform.getKey -> PlatformConfig(platform.getValue))
    }
    PlatformConfigMap(platformMap)
  }

  def apply(platformConfigMap: String): PlatformConfigMap = {
    val platformConfigMapProto = PlatformConfigProtos.PlatformConfigMap.parseFrom(platformConfigMap.getBytes)
    val platformMap = mutable.HashMap[String, PlatformConfig]()
    val it = platformConfigMapProto.getPlatformConfigsMap.entrySet().iterator()
    while (it.hasNext){
      val platform = it.next()
      platformMap += (platform.getKey -> PlatformConfig(platform.getValue))
    }
    PlatformConfigMap(platformMap)
  }
}
