package com.tencent.angel.serving.servables.jpmml

import com.tencent.angel.config.ResourceAllocation
import com.tencent.angel.servable.SavedModelBundleSourceAdapterConfigProtos.SavedModelBundleSourceAdapterConfig
import com.tencent.angel.servable.SessionBundleSourceAdapterConfigProtos.SessionBundleSourceAdapterConfig
import com.tencent.angel.serving.core._
import com.tencent.angel.serving.servables.common.SavedModelBundle

class PMMLSourceAdapter private() extends UnarySourceAdapter[Loader, StoragePath]{
  override def convert(data: ServableData[StoragePath]): ServableData[Loader] = {
    val path = data.data

    val servableCreator: () => SavedModelBundle = () => {
      PMMLSavedModelBundle.create(path)   // load
    }

    val resourceEstimate: () => ResourceAllocation = () => {
      val estimate = PMMLSavedModelBundle.resourceEstimate(path)
      // val options = ResourceOptions(mutable.Map(DeviceType.kMain -> 1))
      //todo: Add experimental_transient_ram_bytes_during_load.
      // val resourceUtil = new ResourceUtil(options)
      estimate
    }

    val postLoadResourceEdtimate: () => ResourceAllocation = () => {
      PMMLSavedModelBundle.estimateResourceRequirement(path)
    }

    new ServableData[Loader](data.id, SimpleLoader[SavedModelBundle](
      servableCreator, resourceEstimate, postLoadResourceEdtimate))
  }
}

object PMMLSourceAdapter {
  def apply(config: SavedModelBundleSourceAdapterConfig): PMMLSourceAdapter = {
    //todo:config.legacy_config()
    new PMMLSourceAdapter()
  }

  def apply(config: SessionBundleSourceAdapterConfig): PMMLSourceAdapter = {
    //todo:config.legacy_config()
    new PMMLSourceAdapter()
  }
}
