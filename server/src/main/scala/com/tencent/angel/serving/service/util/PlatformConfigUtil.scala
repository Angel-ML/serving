package com.tencent.angel.serving.service.util

import com.google.protobuf.Any
import com.tencent.angel.config.PlatformConfigProtos.{PlatformConfig, PlatformConfigMap}
import com.tencent.angel.servable.SavedModelBundleSourceAdapterConfigProtos.SavedModelBundleSourceAdapterConfig
import com.tencent.angel.servable.SessionBundleSourceAdapterConfigProtos.SessionBundleSourceAdapterConfig
import com.tencent.angel.serving.serving.SessionBundleConfig

object PlatformConfigUtil {
  def createAngelPlatformConfigMap(sessionBundleConfig: SessionBundleConfig,
                                   useSavedModel:Boolean): PlatformConfigMap = {
    var sourceAdapterConfig: Any = null
    if(useSavedModel) {
      val savedModelBundleSourceAdapterConfig = SavedModelBundleSourceAdapterConfig
        .newBuilder().setLegacyConfig(sessionBundleConfig).build()
      sourceAdapterConfig = Any.pack(savedModelBundleSourceAdapterConfig)
    } else {
      val sessionBundleSourceAdapterConfig = SessionBundleSourceAdapterConfig
        .newBuilder().setConfig(sessionBundleConfig).build()
      sourceAdapterConfig = Any.pack(sessionBundleSourceAdapterConfig)
    }
    PlatformConfigMap.newBuilder()
      .putPlatformConfigs("Angel", PlatformConfig.newBuilder().setSourceAdapterConfig(sourceAdapterConfig).build())
      .build()
  }
}
