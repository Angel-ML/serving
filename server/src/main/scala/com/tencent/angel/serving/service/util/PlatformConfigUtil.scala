package com.tencent.angel.serving.service.util

import com.google.protobuf.Any
import com.tencent.angel.config.PlatformConfigProtos.{PlatformConfig, PlatformConfigMap}
import com.tencent.angel.servable.SavedModelBundleSourceAdapterConfigProtos.SavedModelBundleSourceAdapterConfig
import com.tencent.angel.servable.SessionBundleSourceAdapterConfigProtos.SessionBundleSourceAdapterConfig
import com.tencent.angel.serving.serving.SessionBundleConfig

object PlatformConfigUtil {
  def createAngelPlatformConfigMap(sessionBundleConfig: SessionBundleConfig,
                                   useSavedModel:Boolean): PlatformConfigMap = {
    val sourceAdapterConfig: Any = if(useSavedModel) {
      val savedModelBundleSourceAdapterConfig = SavedModelBundleSourceAdapterConfig.newBuilder()
        .setAdapterClassName("com.tencent.angel.serving.servables.angel.SavedModelBundleSourceAdapter")
        .setLegacyConfig(sessionBundleConfig)
        .build()
      Any.pack(savedModelBundleSourceAdapterConfig)
    } else {
      val sessionBundleSourceAdapterConfig = SessionBundleSourceAdapterConfig.newBuilder()
        .setAdapterClassName("com.tencent.angel.serving.servables.angel.SavedModelBundleSourceAdapter")
        .setConfig(sessionBundleConfig)
        .build()

      Any.pack(sessionBundleSourceAdapterConfig)
    }

    PlatformConfigMap.newBuilder()
      .putPlatformConfigs("Angel", PlatformConfig.newBuilder().setSourceAdapterConfig(sourceAdapterConfig).build())
      .build()
  }
}
