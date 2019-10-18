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
package com.tencent.angel.serving.servables.angel

import java.io.File

import com.tencent.angel.config.ResourceAllocation
import com.tencent.angel.servable.SavedModelBundleSourceAdapterConfigProtos.SavedModelBundleSourceAdapterConfig
import com.tencent.angel.servable.SessionBundleSourceAdapterConfigProtos.SessionBundleSourceAdapterConfig
import com.tencent.angel.serving.core._
import com.tencent.angel.serving.servables.common.SavedModelBundle


class AngelSourceAdapter private() extends UnarySourceAdapter[Loader, StoragePath] {

  override def convert(data: ServableData[StoragePath]): ServableData[Loader] = {
    val path = data.data

    val servableCreator: () => SavedModelBundle = () => {
      AngelSavedModelBundle.create(path)   // load
    }

    val resourceEstimate: () => ResourceAllocation = () => {
      val estimate = AngelSavedModelBundle.resourceEstimate(path)
      // val options = ResourceOptions(mutable.Map(DeviceType.kMain -> 1))
      //todo: Add experimental_transient_ram_bytes_during_load.
      // val resourceUtil = new ResourceUtil(options)
      estimate
    }

    val postLoadResourceEdtimate: () => ResourceAllocation = () => {
      AngelSavedModelBundle.estimateResourceRequirement(path)
    }

    new ServableData[Loader](data.id, SimpleLoader[SavedModelBundle](
      servableCreator, resourceEstimate, postLoadResourceEdtimate))
  }

  def create(): Unit = ??? //SessionBundleSourceAdapterConfig

  def getCreator() = ???

}

object AngelSourceAdapter {
  def apply(config: SavedModelBundleSourceAdapterConfig): AngelSourceAdapter = {
    //todo:config.legacy_config()
    new AngelSourceAdapter()
  }

  def apply(config: SessionBundleSourceAdapterConfig): AngelSourceAdapter = {
    //todo:config.legacy_config()
    new AngelSourceAdapter()
  }
}

