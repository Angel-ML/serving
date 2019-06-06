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
    val sparkAdapterClassName = "com.tencent.angel.serving.servables.spark.SparkSourceAdapter"
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
      } else if (platform.toLowerCase.equals("spark")) {
        val sourceAdapterConfig: Any = if (useSavedModel) {
          val savedModelBundleSourceAdapterConfig = SavedModelBundleSourceAdapterConfig.newBuilder()
            .setAdapterClassName(sparkAdapterClassName)
            .setLegacyConfig(sessionBundleConfig)
            .build()
          Any.pack(savedModelBundleSourceAdapterConfig)
        } else {
          val sessionBundleSourceAdapterConfig = SessionBundleSourceAdapterConfig.newBuilder()
            .setAdapterClassName(sparkAdapterClassName)
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
