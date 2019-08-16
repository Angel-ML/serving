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

import com.tencent.angel.config.ResourceAllocation
import com.tencent.angel.config.ModelServerConfigProtos.ModelConfigList
import com.tencent.angel.config.ModelServerConfigProtos.ModelServerConfig.ConfigCase
import com.tencent.angel.config.PlatformConfigProtos.PlatformConfigMap
import com.tencent.angel.serving.core.ServerCore.SourceAdapters
import com.tencent.angel.serving.core.metrics.MetricsManager
import com.tencent.angel.serving.serving.ModelServerConfig
import org.apache.commons.io.FilenameUtils

import scala.collection.JavaConverters._


abstract class CoreContext(val eventBus: EventBus[ServableState],
                           val monitor: ServableStateMonitor,
                           val totalResources: ResourceAllocation,
                           var platformConfigMap: PlatformConfigMap,
                           policyClassName: String = "AvailabilityPreservingPolicy"
                          ) {
  var modelConfigListRootDir: String = ""
  var fileSystemPollWaitSeconds: Int = 30
  var manageStateDelayMicros: Long = 1000
  var manageStateIntervalMicros: Long = 60000
  var numLoadThreads: Int = 3
  var numUnloadThreads: Int = 3
  var maxNumLoadRetries: Int = 3
  var loadRetryIntervalMicros: Long = 60000
  var failIfNoModelVersionsFound: Boolean = false
  var flushFilesystemCaches: Boolean = true
  var allowVersionLabels: Boolean = true
  var numInitialLoadThreads: Int = 4 * 1//4 * NumSchedulableCPUs
  val aspiredVersionPolicy: AspiredVersionPolicy = AspiredVersionPolicy(policyClassName)

  var manager: AspiredVersionsManager = _

  var metricSummaryWaitSeconds: Int = 30
  var countDistributionBucket: String = "5,10,15"
  var enableMetricSummary: Boolean = false
  var targetPublishingMetric: String = "logger"
  var metricsManager: MetricsManager = _

  def addModelsViaModelConfigList(config: ModelServerConfig): Unit

  def customModelConfigLoader: CustomModelConfigLoader

  def maybeUpdateServerRequestLogger(config: ModelServerConfig): Unit

  protected def waitUntilModelsAvailable(models: Set[String], monitor: ServableStateMonitor): Unit = {
    val awaitedServables:List[ServableRequest] = models.map(ServableRequest.latest(_)).toList
    val statesReached = monitor.waitUntilServablesReachState(awaitedServables, ManagerState.kAvailable)
    if(statesReached.isEmpty){
      val numUnavailableModels = statesReached.count(stateReached => stateReached._2 != ManagerState.kAvailable)
      val message = String.join(numUnavailableModels.toString,"model(s) did not become avaible:")
      statesReached.collect{case (servableId, managerState) if (managerState != ManagerState.kAvailable)=>
        message.concat(s"{${servableId.toString}}")}
      throw  new Exception(message)
    }
  }

  protected def connectAdaptersToManagerAndAwaitModelLoads(adapters: SourceAdapters,
                                                           config: ModelServerConfig): Unit = {
    val modelsToAwait = config.getModelConfigList.getConfigList.asScala.map { modelConfig =>
      ServableRequest.earliest(modelConfig.getName)
    }.toList

    val adapterList = adapters.platformAdapters.map { case (_, adapter) =>
      adapter.asInstanceOf[Source[Loader]]
    }.toList
    LoadServablesFast.connectSourcesWithFastInitialLoad(manager, adapterList, monitor, modelsToAwait, numInitialLoadThreads)
  }
}