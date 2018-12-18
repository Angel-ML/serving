package com.tencent.angel.serving.core


import com.tencent.angel.config.ResourceAllocation
import com.tencent.angel.servable.SavedModelBundleSourceAdapterConfigProtos.SavedModelBundleSourceAdapterConfig
import com.tencent.angel.serving.servables.angel.SavedModelBundle

import scala.collection.mutable


class SavedModelBundleSourceAdapter(bundleFactory: SavedModelBundleFactory) extends UnarySourceAdapter[Loader, StoragePath] {


  override def convert(data: ServableData[StoragePath]): ServableData[Loader] = {
    val servableCreator: Unit => SavedModelBundle = () => {
      bundleFactory.createSavedModelBundle(data.data)
    }
    val resourceEstimate: Unit => ResourceAllocation = () => {
      val estimate = bundleFactory.estimateResourceRequirement(data.data)
      // val options = ResourceOptions(mutable.Map(DeviceType.kMain -> 1))
      //todo: Add experimental_transient_ram_bytes_during_load.
      // val resourceUtil = new ResourceUtil(options)
      estimate
    }
    val postLoadResourceEdtimate: Unit => ResourceAllocation = () => {
      bundleFactory.estimateResourceRequirement(data.data)
    }
    new ServableData[Loader](data.id, SimpleLoader[SavedModelBundle](
      servableCreator, resourceEstimate, postLoadResourceEdtimate))
  }

  def create(): Unit = ??? //SessionBundleSourceAdapterConfig

  def getCreator() = ???


}

object SavedModelBundleSourceAdapterConfigCreator {
  def create(config: SavedModelBundleSourceAdapterConfig) = {
    val bundleFactory = SavedModelBundleFactory.create(config.getLegacyConfig) //todo:config.legacy_config()
    new SavedModelBundleSourceAdapter(bundleFactory)
  }
}

