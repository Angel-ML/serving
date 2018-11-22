package com.tencent.angel.serving.core

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.config.ModelServerConfigProtos.ModelServerConfig.ConfigCase
import com.tencent.angel.serving.serving.{ModelServerConfig, _}
import com.tencent.angel.serving.sources.FileSystemStoragePathSource
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

case class ServerOptions()

class ServerCore(val options: ServerOptions) extends Manager {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[ServerCore])

  private val platform2RouterPort = new mutable.HashMap[String, Int]()
  private val servableRventBus = EventBus[ServableState]()
  private val aspiredVersionPolicy = new ServableStateMonitor(servableRventBus, 1000)
  private val aspiredVersionPolicy: AspiredVersionPolicy = _
  private val manager: AspiredVersionsManager = new AspiredVersionsManager()

  private var config: ModelServerConfig = _
  private val modelLabelsToVersions = new mutable.HashMap[String, mutable.HashMap[String, Int]]()

  //-------------------------------------------------------------------------Server Setup and Initialization
  private def initialize(policy: AspiredVersionPolicy): Unit = {
    // 1. initial aspiredVersionPolicy

    // 2. initial manager(AspiredVersionsManager)
  }

  private def reloadConfig(newConfig: ModelServerConfig): Unit = {
    val isFirstConfig = config.getConfigCase == ConfigCase.CONFIG_NOT_SET
    val acceptTransition = isFirstConfig || (ConfigCase.MODEL_CONFIG_LIST == newConfig.getConfigCase &&
      ConfigCase.MODEL_CONFIG_LIST == config.getConfigCase)

    if (!acceptTransition) {
      throw ConfigExceptions("not acceptTransition!")
    }

    // Nothing to load. In this case we allow a future call with a non-empty config.
    if (newConfig.getConfigCase == ConfigCase.CONFIG_NOT_SET) {
      LOG.info("Taking no action for empty config.")
      return
    }

    if (newConfig.getConfigCase == ConfigCase.MODEL_CONFIG_LIST) {
      validateModelConfigList(newConfig, options)
    }

    if (ConfigCase.MODEL_CONFIG_LIST == newConfig.getConfigCase &&
      ConfigCase.MODEL_CONFIG_LIST == config.getConfigCase) {
      validateNoModelsChangePlatforms()
    }

    config = newConfig

    LOG.info("UpdateModelVersionLabelMap")
    updateModelVersionLabelMap()

    LOG.info("Adding/updating models.")
    config.getConfigCase match {
      case ConfigCase.MODEL_CONFIG_LIST =>
        updateModelConfigListRelativePaths()
        addModelsViaModelConfigList()
      case ConfigCase.CUSTOM_MODEL_CONFIG =>
        addModelsViaCustomModelConfig()
    }

    maybeUpdateServerRequestLogger()
  }

  private def updateModelVersionLabelMap(): Unit = {

  }


  //-------------------------------------------------------------------------Manager
  override def availableServableIds: List[ServableId] = {
    manager.availableServableIds
  }

  override def availableServableHandles[Loader]: Map[ServableId, ServableHandle[Loader]] = {
    manager.availableServableHandles
  }

  override def servableHandle[Loader](request: ServableRequest): ServableHandle[Loader] = {
    manager.servableHandle(request)
  }

  override def untypedServableHandle(request: ServableRequest): UntypedServableHandle = {
    manager.untypedServableHandle(request)
  }

  override def availableUntypedServableHandles: Map[ServableId, UntypedServableHandle] = {
    manager.availableUntypedServableHandles
  }


  //-------------------------------------------------------------------------Request Processing
  def servableRequestFromModelSpec(modelSpec: ModelSpec): ServableRequest = ???

  def getModelVersionForLabel(modelName: String, label: String): Long = ???
}

object ServerCore {

  case class SourceAdapters(platformAdapters: Map[String, StoragePathSourceAdapter],
                            errorAdapter: StoragePathSourceAdapter)

  def apply(): ServerCore = new ServerCore()

  def createAspiredVersionsManager(policy: AspiredVersionPolicy): AspiredVersionsManager = ???

  def createResourceTracker(): ResourceTracker = ???

  def createAdapter(modelPlatform: String): StoragePathSourceAdapter = ???

  def createStoragePathSourceConfig(config: ModelServerConfig): FileSystemStoragePathSourceConfig = ???

  def createStoragePathRoutes(config: ModelServerConfig): Routes = ???

  def createStoragePathSource(config: FileSystemStoragePathSourceConfig,
                              target: Target[StoragePath]): FileSystemStoragePathSource = ???

  def createRouter(routes: Routes, tragets: SourceAdapters): DynamicSourceRouter[StoragePath] = ???

  def createAdapters(): SourceAdapters = ???
}
