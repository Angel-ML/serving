package com.tencent.angel.serving.core

import com.tencent.angel.config.ResourceAllocation
import com.tencent.angel.config.ModelServerConfigProtos.ModelConfigList
import com.tencent.angel.config.ModelServerConfigProtos.ModelServerConfig.ConfigCase
import com.tencent.angel.config.PlatformConfigProtos.PlatformConfigMap
import com.tencent.angel.serving.core.ServerCore.SourceAdapters
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
  var manageStateIntervalMicros: Long = 120000
  var numLoadThreads: Int = 3
  var numUnloadThreads: Int = 3
  var maxNumLoadRetries: Int = 3
  var loadRetryIntervalMicros: Long = 60000
  var failIfNoModelVersionsFound: Boolean = false
  var allowVersionLabels: Boolean = true
  var numInitialLoadThreads: Int = 4 * 1//4 * NumSchedulableCPUs
  val aspiredVersionPolicy: AspiredVersionPolicy = AspiredVersionPolicy(policyClassName)

  var manager: AspiredVersionsManager = _

  def addModelsViaModelConfigList(config: ModelServerConfig): Unit

  def customModelConfigLoader: CustomModelConfigLoader

  def maybeUpdateServerRequestLogger(configCase: ConfigCase): Unit

  protected def waitUntilModelsAvailable(models: Set[String], monitor: ServableStateMonitor): Unit = {
    val awaitedServables:List[ServableRequest] = models.map(ServableRequest.latest(_)).toList
    val statesReached = monitor.waitUntilServablesReachState(awaitedServables, ManagerState.kAvailable)
    val numUnavailableModels = statesReached.count(stateReached => stateReached._2 != ManagerState.kAvailable)
    val message = String.join(numUnavailableModels.toString,"models did not become avaible:")
    statesReached.collect{case (servableId, managerState) if (managerState != ManagerState.kAvailable)=>
      message.concat(s"{${servableId.toString}}")}
    throw  new Exception(message)
  }

  protected def connectAdaptersToManagerAndAwaitModelLoads(adapters: SourceAdapters,
                                                           config: ModelServerConfig): Unit = {
    val modelsToAwait = config.getModelConfigList.getConfigList.asScala.map { modelConfig =>
      ServableRequest.earliest(modelConfig.getName)
    }.toList

    val adapterList = adapters.platformAdapters.map { case (_, adapter) =>
      adapter.asInstanceOf[Source[Loader]]
    }.toList
    connectSourcesWithFastInitialLoad(adapterList, modelsToAwait, numInitialLoadThreads)
  }

  protected def connectSourcesWithFastInitialLoad(sources: List[Source[Loader]],
                                                  initialServables: List[ServableRequest],
                                                  numThreads: Int): Unit = {

  }
}