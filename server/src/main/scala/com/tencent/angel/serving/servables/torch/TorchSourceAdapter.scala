package com.tencent.angel.serving.servables.torch

import com.tencent.angel.config.ResourceAllocation
import com.tencent.angel.servable.SavedModelBundleSourceAdapterConfigProtos.SavedModelBundleSourceAdapterConfig
import com.tencent.angel.servable.SessionBundleSourceAdapterConfigProtos.SessionBundleSourceAdapterConfig
import com.tencent.angel.serving.core._
import com.tencent.angel.serving.servables.common.SavedModelBundle

class TorchSourceAdapter private() extends UnarySourceAdapter[Loader, StoragePath]{
  override def convert(data: ServableData[StoragePath]): ServableData[Loader] = {
    val path = data.data

    val servableCreator: () => SavedModelBundle = () => {
      TorchSavedModelBundle.create(path)   // load
    }

    val resourceEstimate: () => ResourceAllocation = () => {
      val estimate = TorchSavedModelBundle.resourceEstimate(path)
      // val options = ResourceOptions(mutable.Map(DeviceType.kMain -> 1))
      //todo: Add experimental_transient_ram_bytes_during_load.
      // val resourceUtil = new ResourceUtil(options)
      estimate
    }

    val postLoadResourceEdtimate: () => ResourceAllocation = () => {
      TorchSavedModelBundle.estimateResourceRequirement(path)
    }

    new ServableData[Loader](data.id, SimpleLoader[SavedModelBundle](
      servableCreator, resourceEstimate, postLoadResourceEdtimate))
  }
}

object TorchSourceAdapter {
  def apply(config: SavedModelBundleSourceAdapterConfig): TorchSourceAdapter = {
    //todo:config.legacy_config()
    new TorchSourceAdapter()
  }

  def apply(config: SessionBundleSourceAdapterConfig): TorchSourceAdapter = {
    //todo:config.legacy_config()
    new TorchSourceAdapter()
  }
}
