package com.tencent.angel.serving.core

import com.tencent.angel.confg.ResourceAllocation
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
  var numInitialLoadThreads: Int = 4 * 1
  val aspiredVersionPolicy: AspiredVersionPolicy = AspiredVersionPolicy(policyClassName)

  var manager: AspiredVersionsManager = _

  def addModelsViaModelConfigList(config: ModelServerConfig): Unit

  def customModelConfigLoader: CustomModelConfigLoader

  def maybeUpdateServerRequestLogger(configCase: ConfigCase): Unit

  protected def waitUntilModelsAvailable(models: Set[String], monitor: ServableStateMonitor): Unit = ???

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

  protected def updateModelConfigListRelativePaths(modelConfigListRootDir: String,
                                                   modelConfigList: ModelConfigList): ModelConfigList = {
    val builder: ModelConfigList.Builder = modelConfigList.toBuilder

    modelConfigList.getConfigList.asScala.zipWithIndex.foreach { case (modelConfig, idx) =>
      val basePath = modelConfig.getBasePath
      // Don't modify absolute paths.
      if (ServerCore.uriIsRelativePath(basePath)) {
        val fullPath = FilenameUtils.concat(modelConfigListRootDir, basePath)
        if (ServerCore.uriIsRelativePath(fullPath)) {
          throw InvalidArguments(s"Expected model ${modelConfig.getName}, with updated base_path = " +
            s"JoinPath($modelConfigListRootDir, $basePath) to have an absolute path; got $fullPath")
        }

        val newModelConfig = builder.getConfigBuilder(idx).setBasePath(fullPath).build()
        builder.setConfig(idx, newModelConfig)
      }
    }

    builder.build()
  }

  protected def connectSourcesWithFastInitialLoad(sources: List[Source[Loader]],
                                                  initialServables: List[ServableRequest],
                                                  numThreads: Int): Unit = {

  }
}