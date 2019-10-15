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
package com.tencent.angel.serving.core

import com.tencent.angel.config.{Entry, Resource, ResourceAllocation}
import com.tencent.angel.serving.servables.common.SavedModelBundle

import scala.collection.mutable
import scala.language.reflectiveCalls

class SimpleLoader[ServableType <: SavedModelBundle](creator: () => ServableType,
                                                     resourceEstimate: () => ResourceAllocation) extends Loader {

  private var postLoadResourceEstimate: () => ResourceAllocation = _
  private var memorizedResourceEstimator: ResourceAllocation = _
  private var resourceUtil: ResourceUtil = _
  // private var ramResource: Resource = null
  private var servable_ : ServableType = _


  override def estimateResources(): ResourceAllocation = {
    val run = Runtime.getRuntime
    val available = (run.totalMemory() * 0.8 * 0.2).toLong // byte
    ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), available)))
  }

  override def load(): Unit = {
    servable_ = creator()
    if (postLoadResourceEstimate != null) {
      // var duringLoadResourceEstimate: ResourceAllocation = estimateResources()
      memorizedResourceEstimator = postLoadResourceEstimate()
      //todo: Release any transient memory used only during load to the OS
    }
  }

  override def unload(): Unit = {
    // val resourceEstimate = estimateResources()
    servable_.unLoad()
    servable_ = null.asInstanceOf[ServableType]
    //todo: release resource
  }

  override def servable(): Any = servable_
}

object SimpleLoader {
  def apply[ServableType <: SavedModelBundle](creator: () => ServableType, resourceEstimate: () => ResourceAllocation
                                             ): SimpleLoader[ServableType] = {
    val loader = new SimpleLoader[ServableType](creator, resourceEstimate)
    val resourceOptions = ResourceOptions(mutable.Map(DeviceType.kMain -> 1))
    loader.resourceUtil = new ResourceUtil(resourceOptions)
    //todo: ramResource
    loader
  }

  def apply[ServableType <: SavedModelBundle](creator: () => ServableType, resourceEstimate: () => ResourceAllocation,
                                              postLoadResourceEstimate: () => ResourceAllocation): SimpleLoader[ServableType] = {
    val loader = SimpleLoader(creator, resourceEstimate)
    loader.postLoadResourceEstimate = postLoadResourceEstimate
    loader
  }
}
