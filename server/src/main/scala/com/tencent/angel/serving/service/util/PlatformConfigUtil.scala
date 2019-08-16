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
package com.tencent.angel.serving.service.util

import com.google.protobuf.Any
import com.tencent.angel.config.PlatformConfigProtos.{PlatformConfig, PlatformConfigMap}
import com.tencent.angel.servable.SavedModelBundleSourceAdapterConfigProtos.SavedModelBundleSourceAdapterConfig
import com.tencent.angel.servable.SessionBundleSourceAdapterConfigProtos.SessionBundleSourceAdapterConfig
import com.tencent.angel.serving.serving.SessionBundleConfig

object PlatformConfigUtil {
  def createPlatformConfigMap(sessionBundleConfig: SessionBundleConfig,
                                   useSavedModel:Boolean, platformSet: Set[String]): PlatformConfigMap = {
    val builder = PlatformConfigMap.newBuilder()
    val angelAdapterClassName = "com.tencent.angel.serving.servables.angel.AngelSourceAdapter"
    val jpmmlAdapterClassName = "com.tencent.angel.serving.servables.jpmml.PMMLSourceAdapter"
    val torchAdapterClassName = "com.tencent.angel.serving.servables.torch.TorchSourceAdapter"
    platformSet.foreach { platform =>
      if (platform.toLowerCase.equals("angel")) {
        val sourceAdapterConfig: Any = if (useSavedModel) {
          val savedModelBundleSourceAdapterConfig = SavedModelBundleSourceAdapterConfig.newBuilder()
            .setAdapterClassName(angelAdapterClassName)
            .setLegacyConfig(sessionBundleConfig)
            .build()
          Any.pack(savedModelBundleSourceAdapterConfig)
        } else {
          val sessionBundleSourceAdapterConfig = SessionBundleSourceAdapterConfig.newBuilder()
            .setAdapterClassName(angelAdapterClassName)
            .setConfig(sessionBundleConfig)
            .build()

          Any.pack(sessionBundleSourceAdapterConfig)
        }
        builder
          .putPlatformConfigs(platform, PlatformConfig.newBuilder().setSourceAdapterConfig(sourceAdapterConfig).build())
      } else if (platform.toLowerCase.equals("pmml")) {
        val sourceAdapterConfig: Any = if (useSavedModel) {
          val savedModelBundleSourceAdapterConfig = SavedModelBundleSourceAdapterConfig.newBuilder()
            .setAdapterClassName(jpmmlAdapterClassName)
            .setLegacyConfig(sessionBundleConfig)
            .build()
          Any.pack(savedModelBundleSourceAdapterConfig)
        } else {
          val sessionBundleSourceAdapterConfig = SessionBundleSourceAdapterConfig.newBuilder()
            .setAdapterClassName(jpmmlAdapterClassName)
            .setConfig(sessionBundleConfig)
            .build()

          Any.pack(sessionBundleSourceAdapterConfig)
        }
        builder
          .putPlatformConfigs(platform, PlatformConfig.newBuilder().setSourceAdapterConfig(sourceAdapterConfig).build())
      } else if (platform.toLowerCase.equals("torch")) {
        val sourceAdapterConfig: Any = if (useSavedModel) {
          val savedModelBundleSourceAdapterConfig = SavedModelBundleSourceAdapterConfig.newBuilder()
            .setAdapterClassName(torchAdapterClassName)
            .setLegacyConfig(sessionBundleConfig)
            .build()
          Any.pack(savedModelBundleSourceAdapterConfig)
        } else {
          val sessionBundleSourceAdapterConfig = SessionBundleSourceAdapterConfig.newBuilder()
            .setAdapterClassName(torchAdapterClassName)
            .setConfig(sessionBundleConfig)
            .build()

          Any.pack(sessionBundleSourceAdapterConfig)
        }
        builder
          .putPlatformConfigs(platform, PlatformConfig.newBuilder().setSourceAdapterConfig(sourceAdapterConfig).build())
      }
    }
    builder.build()
  }
}
