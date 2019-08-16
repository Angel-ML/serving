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

import com.tencent.angel.config.{Entry, ResourceAllocation}
import org.slf4j.{Logger, LoggerFactory}

class ResourceTracker(val totalResources: ResourceAllocation, maxNumLoadRetries: Int, loadRetryIntervalMicros: Long) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[ResourceTracker])

  val usedResources: ResourceAllocation = new ResourceAllocation(totalResources.resourceQuantities.map{ entry =>
    Entry(entry.resource, 0)})
  val retry = new Retry(maxNumLoadRetries, loadRetryIntervalMicros)

  def reserveResources(harness: LoaderHarness): Boolean = this.synchronized {
    val retriedFn = () => {
      LOG.info("[reserveResources] 1. estimateResources")
      val resources = harness.loader.estimateResources()
      LOG.info("[reserveResources] 2. verify and ensure the resources is satisfied")
      if (resources.verify()) {
        if (overbind(usedResources) + resources < totalResources) {
          true
        } else {
          false
        }
      } else {
        false
      }
    }

    val isCancelledFn = () => {
      harness.isRetryCanceled
    }

    retry(retriedFn, isCancelledFn)
  }

  def recomputeUsedResources(servables: List[LoaderHarness]): Unit = this.synchronized {
    usedResources.clear()
    servables.foreach { harness =>
      val resources = harness.loader.estimateResources()
      if (resources.verify()) {
        usedResources += resources
      } else {
        // resource error
        throw ResourceExceptions("estimateResources Error!")
      }
    }
  }


  private def overbind(servableResources: ResourceAllocation): ResourceAllocation = servableResources * 1.2
}


