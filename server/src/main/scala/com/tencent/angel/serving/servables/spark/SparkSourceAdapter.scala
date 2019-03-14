package com.tencent.angel.serving.servables.spark

import com.tencent.angel.serving.core.{Loader, ServableData, StoragePath, UnarySourceAdapter}
import com.tencent.angel.config.ResourceAllocation
import com.tencent.angel.servable.SavedModelBundleSourceAdapterConfigProtos.SavedModelBundleSourceAdapterConfig
import com.tencent.angel.servable.SessionBundleSourceAdapterConfigProtos.SessionBundleSourceAdapterConfig
import com.tencent.angel.serving.core._
import com.tencent.angel.serving.servables.common.SavedModelBundle

class SparkSourceAdapter extends UnarySourceAdapter[Loader, StoragePath] {
  override def convert(data: ServableData[StoragePath]): ServableData[Loader] = {
    val path = data.data

    val servableCreator: () => SparkSavedModelBundle = () => {
      SparkSavedModelBundle.create(path)   // load
    }

    val resourceEstimate: () => ResourceAllocation = () => {
      val estimate = SparkSavedModelBundle.resourceEstimate(path)
      // val options = ResourceOptions(mutable.Map(DeviceType.kMain -> 1))
      //todo: Add experimental_transient_ram_bytes_during_load.
      // val resourceUtil = new ResourceUtil(options)
      estimate
    }

    val postLoadResourceEdtimate: () => ResourceAllocation = () => {
      SparkSavedModelBundle.estimateResourceRequirement(path)
    }

    new ServableData[Loader](data.id, SimpleLoader[SavedModelBundle](
      servableCreator, resourceEstimate, postLoadResourceEdtimate))
  }
}


object SparkSourceAdapter {

  def apply(config: SavedModelBundleSourceAdapterConfig): SparkSourceAdapter = {
    //todo:config.legacy_config()
    new SparkSourceAdapter()
  }

  def apply(config: SessionBundleSourceAdapterConfig): SparkSourceAdapter = {
    //todo:config.legacy_config()
    new SparkSourceAdapter()
  }
}