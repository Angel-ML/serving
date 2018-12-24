package com.tencent.angel.serving.servables.angel

import java.io.File

import com.tencent.angel.config.ResourceAllocation
import com.tencent.angel.servable.SavedModelBundleSourceAdapterConfigProtos.SavedModelBundleSourceAdapterConfig
import com.tencent.angel.servable.SessionBundleSourceAdapterConfigProtos.SessionBundleSourceAdapterConfig
import com.tencent.angel.serving.core._
import com.tencent.angel.serving.servables.common.SavedModelBundle


class SavedModelBundleSourceAdapter private() extends UnarySourceAdapter[Loader, StoragePath] {

  override def convert(data: ServableData[StoragePath]): ServableData[Loader] = {
    val path = data.data

    val servableCreator: () => SavedModelBundle = () => {
      AngelSavedModelBundle(path)   // load
    }

    val resourceEstimate: () => ResourceAllocation = () => {
      val estimate = SavedModelBundle.estimateResourceRequirement(path)
      // val options = ResourceOptions(mutable.Map(DeviceType.kMain -> 1))
      //todo: Add experimental_transient_ram_bytes_during_load.
      // val resourceUtil = new ResourceUtil(options)
      estimate
    }

    val postLoadResourceEdtimate: () => ResourceAllocation = () => {
      SavedModelBundle.estimateResourceRequirement(path)
    }

    new ServableData[Loader](data.id, SimpleLoader[SavedModelBundle](
      servableCreator, resourceEstimate, postLoadResourceEdtimate))
  }

  def create(): Unit = ??? //SessionBundleSourceAdapterConfig

  def getCreator() = ???

}

object SavedModelBundleSourceAdapter {
  def apply(config: SavedModelBundleSourceAdapterConfig): SavedModelBundleSourceAdapter = {
    //todo:config.legacy_config()
    new SavedModelBundleSourceAdapter()
  }

  def apply(config: SessionBundleSourceAdapterConfig): SavedModelBundleSourceAdapter = {
    //todo:config.legacy_config()
    new SavedModelBundleSourceAdapter()
  }
}

