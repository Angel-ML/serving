package com.tencent.angel.serving.core

import java.net.URI
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.tencent.angel.config.FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig
import com.tencent.angel.config.FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableToMonitor
import com.tencent.angel.config.ModelServerConfigProtos.ModelServerConfig.ConfigCase
import com.tencent.angel.config.ModelServerConfigProtos.{ModelConfig, ModelConfigList, ModelServerConfig}
import com.tencent.angel.config.PlatformConfigProtos.{PlatformConfig, PlatformConfigMap}
import com.tencent.angel.serving.core.ManagerState.ManagerState
import com.tencent.angel.serving.core.ServableStateMonitor.{ServableMap, ServableStateAndTime, VersionMap}
import com.tencent.angel.serving.serving._
import com.tencent.angel.serving.sources.FileSystemStoragePathSource
import org.apache.commons.io.FilenameUtils
import org.apache.commons.logging.LogFactory

case class ServerOptions(){
  var modelServerConfig: ModelServerConfig = ???
  var modelConfigListRootDir: String = ???
  var aspiredVersionPolicy: AspiredVersionPolicy = ???
  var fileSystemPollWaitSeconds: Int = 30
  var failIfNoModelVersionsFound: Boolean = false
  var allowVersionLabels:Boolean = true
  var platformConfigMap: PlatformConfigMap = ???
  var numInitialLoadThreads: Int = 4 * 1
  var servableStateMonitorCreator: ServableStateMonitorCreator = ???
  var customModelConfigLoader: CustomModelConfigLoader = ???

}


class ServerCore(val options: ServerOptions) extends Manager {
  override def availableServableIds: List[ServableId] = ???

  override def availableServableHandles[T]: Map[ServableId, ServableHandle[T]] = ???

  override def servableHandle[T](request: ServableRequest): ServableHandle[T] = ???

  override def untypedServableHandle(request: ServableRequest): UntypedServableHandle = ???

  override def availableUntypedServableHandles: Map[ServableId, UntypedServableHandle] = ???
}

object ServerCore {
  val LOG = LogFactory.getLog(classOf[ServerCore])
  var options_ :ServerOptions = ???
  var config_ : ModelServerConfig = ???
  var platformToRouterPort_ : Map[String, Int] = ???
  var modelLabelToVersions_ : Map[String, Map[String, Int]] = ???
  var storagePathSourceAndRouter: StoragePathSourceAndRouter = null
  var servableStateMonitor: ServableStateMonitor = ???
  var servableEventBus: EventBus[ServableState] = ???
  var manager_ : AspiredVersionsManager = ???

  case class StoragePathSourceAndRouter(source: FileSystemStoragePathSource, router: DynamicSourceRouter[StoragePath])

  case class SourceAdapters(platformAdapters: Map[String, StoragePathSourceAdapter],
                            errorAdapter: StoragePathSourceAdapter)

  private val modelLabelToVersionsLock = new ReentrantReadWriteLock()
  private val modelLabelToVersionsReadLock: ReentrantReadWriteLock.ReadLock = modelLabelToVersionsLock.readLock()
  private val modelLabelToVersionsWriteLock: ReentrantReadWriteLock.WriteLock = modelLabelToVersionsLock.writeLock()

  private val configLock = new ReentrantReadWriteLock()
  private val configReadLock: ReentrantReadWriteLock.ReadLock = configLock.readLock()
  private val configWriteLock: ReentrantReadWriteLock.WriteLock = configLock.writeLock()

  def apply(options: ServerOptions): ServerCore = new ServerCore(options)

  def createAspiredVersionsManager(policy: AspiredVersionPolicy): AspiredVersionsManager = ???

  def createResourceTracker(): ResourceTracker = ???

  def createAdapter(modelPlatform: String): StoragePathSourceAdapter = ???

  def createStoragePathSourceConfig(config: ModelServerConfig): FileSystemStoragePathSourceConfig = {
    val servables = new java.util.ArrayList[ServableToMonitor]()
    for ((model:ModelConfig) <- config.getModelConfigList.getConfigList){
      LOG.info(s"(re-)adding model: ${model.getName}")
      val monitorBuilder = ServableToMonitor.newBuilder()
      monitorBuilder.setServableName(model.getName)
      monitorBuilder.setBasePath(model.getBasePath)
      monitorBuilder.setServableVersionPolicy(model.getModelVersionPolicy)
      servables.add(new ServableToMonitor(monitorBuilder))
    }
    val builder = FileSystemStoragePathSourceConfig.newBuilder()
    builder.addAllServables(servables)
    builder.setFileSystemPollWaitSeconds(options_.fileSystemPollWaitSeconds)
    builder.setFailIfZeroVersionsAtStartup(options_.failIfNoModelVersionsFound)
    builder.build()
  }

  def createStoragePathRoutes(config: ModelServerConfig): Routes = {
    var routes:Routes = Map[String, Int]()
    for ((model: ModelConfig) <- config.getModelConfigList.getConfigList){
      val modelName: String = model.getName
      val platform: String = getPlatform(model)
      val port: Int = platformToRouterPort_.get(platform).to[Int]
      if (port == None){
        throw InvalidArguments(s"Model ${modelName},requests unsupported platform ${platform}")
      }
      routes += (modelName -> port)
    }
    routes
  }

  def createStoragePathSource(config: FileSystemStoragePathSourceConfig,
                              target: Target[StoragePath]): FileSystemStoragePathSource = {
    val source = FileSystemStoragePathSource.create(config)
    ConnectSourceToTarget(source, target)
    source
  }

  def createRouter(routes: Routes, targets: SourceAdapters): DynamicSourceRouter[StoragePath] = {
    val numOutpurPorts = targets.platformAdapters.size + 1
    val router = DynamicSourceRouter.apply[StoragePath](numOutpurPorts, routes)

    val outputPorts :List[Source[StoragePath]] = router.getOutputPorts
    targets.platformAdapters.foreach{ case (platform, adapter) =>
      val port: Int = platformToRouterPort_.get(platform).to[Int]
      if (port == None){
        throw FailedPreconditions("Router port for platform not found.")
      }
      ConnectSourceToTarget(outputPorts(port), adapter)
    }
    ConnectSourceToTarget(outputPorts.last, targets.errorAdapter)

    router
  }

  def createAdapters(): SourceAdapters = {
    var platformAdapters = Map[String, StoragePathSourceAdapter]()
    platformToRouterPort_.foreach{ case (platform, port) =>
        val adapter: StoragePathSourceAdapter = createAdapter(platform)
        platformAdapters += (platform -> adapter)
    }
    val errorAdapters = new ErrorSourceAdapter[StoragePath, Loader](FailedPreconditions("No platform found for model"))
    SourceAdapters(platformAdapters, errorAdapters)
  }

  def getPlatform(modelConfig: ModelConfig): String = {
    val platform:String = modelConfig.getModelPlatform
    if (platform == ""){
      throw InvalidArguments(s"Illegal setting ModelServerConfig::model_platform.")
    }
    platform
  }

  def reloadConfig(newConfig: ModelServerConfig): Unit ={
    configWriteLock.lock()
    try {
      // Determine whether to accept this config transition.
      val isFirstConfig = config_.getConfigCase == ModelServerConfig.ConfigCase.CONFIG_NOT_SET
      val acceptTransition = isFirstConfig || (config_.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST
        && newConfig.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST)
      if (!acceptTransition){
        throw FailedPreconditions("Cannot transition to requested config. It is only legal to transition " +
          "from one ModelConfigList to another.")
      }
      if (newConfig.getConfigCase == ModelServerConfig.ConfigCase.CONFIG_NOT_SET){
        //Nothing to load. In this case we allow a future call with a non-empty config.
        LOG.info("nothing to load, taking no action fo empty config")
        return
      }
      if (newConfig.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST){
        validateModelConfigList(newConfig.getModelConfigList, options_)
      }
      if(newConfig.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST &&
        config_.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST){
        validateNoModelsChangePlatforms(config_.getModelConfigList, newConfig.getModelConfigList)
      }
      config_ = newConfig
      updateModelVersionLabelMap()

      LOG.info("adding or updating models")
      config_.getConfigCase match {
        case ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST =>{
          if (options_.modelConfigListRootDir != ""){
             updateModelConfigListRelativePaths(options_.modelConfigListRootDir, config_.getModelConfigList)
          }
          addModelsViaModelConfigList()
        }
        case ModelServerConfig.ConfigCase.CUSTOM_MODEL_CONFIG =>{
          addModelsViaCustomModelConfig()
        }
        case _ => throw new Exception("Invalid ServerModelConfig")
      }
      maybeUpdateServerRequestLogger(config_.getConfigCase)

    } finally {
      configWriteLock.unlock()
    }
  }

  def validateModelConfigList(configList: ModelConfigList, options: ServerOptions): Unit = {
    //unique model
    val modelNames = Set[String]()
    for ((model: ModelConfig)<- configList.getConfigList){
      val name = model.getName
      if (modelNames.contains(name)){
        throw InvalidArguments(s"Illegal to list model ${name}, multiple times in config list")
      }
      modelNames.+(name)
    }

    // Base-paths are either all relative, or all absolute.
    // WARNING: abuse of terminology!  These "paths" may be URIs
    if (options.modelConfigListRootDir != ""){
      //all base path must be relative
      if (uriIsRelativePath(options.modelConfigListRootDir)){//todo
        throw InvalidArguments(s"Expected non-empty absolute path or URI; " +
          s"got model_config_list_root_dir= ${options.modelConfigListRootDir}")
      }
    } else {
      // All base-paths must be absolute.
      for((model: ModelConfig)<- configList.getConfigList){
        if (uriIsRelativePath(model.getBasePath)){
          throw InvalidArguments(s"Expected model ${model.getName()} to have an absolute path or URI; " +
            s"got base_path()=${model.getBasePath()}")
        }
      }
    }
  }

  def uriIsRelativePath(uriStr: String): Boolean = {
    val uri = new URI(uriStr)
    !uri.isAbsolute
  }

  def validateNoModelsChangePlatforms(oldConfigList: ModelConfigList, newConfigList: ModelConfigList): Unit = {
    var oldModelPlatforms = Map[String, String]()
    for ((oldConfig: ModelConfig) <- oldConfigList.getConfigList){
      val platform = getPlatform(oldConfig)
      oldModelPlatforms += (oldConfig.getName -> platform)
    }

    var newModelPlatforms = Map[String, String]()
    for ((newConfig: ModelConfig) <- newConfigList.getConfigList){
      val oldPlatform = oldModelPlatforms.get(newConfig.getName)
      if (oldPlatform != None){
        val newPlatform = getPlatform(newConfig)
        if (oldPlatform != newPlatform){
          throw InvalidArguments(s"Illegal to change a model's platform. For model ${newConfig.getName} platform was " +
            s"${oldPlatform}old_platform, and new platform requested is ${newPlatform}")
        }
      }
    }
  }

  def updateModelVersionLabelMap(): Unit = {
    var newLabelMap = Map[String, Map[String, Int]]()
    for ((modelConfig: ModelConfig) <- config_.getModelConfigList.getConfigList){
      val servingStates: VersionMap = servableStateMonitor.getVersionStates(modelConfig.getName)

      modelConfig.getVersionLabelsMap.forEach{ case (label, version) =>
        //Verify that the label points to a version that is currently available.
        val servableStateAndTime = servingStates.get(version)
        if(servableStateAndTime == None || servableStateAndTime.get.state.managerState != ManagerState.kAvailable){
          throw FailedPreconditions(s"Request to assign label to ${version}, version of model ${modelConfig.getName}, " +
            s"which is not currently available for inference")
        }
        newLabelMap += (modelConfig.getName -> (label -> version))
      }
    }

    if (!options_.allowVersionLabels){
      if (!newLabelMap.isEmpty){
        throw new FailedPreconditions("Model version labels are not currently allowed by the server.")
      }
    } else {
      modelLabelToVersionsWriteLock.lock()
      try{
        modelLabelToVersions_ = newLabelMap.clone().asInstanceOf[Map[String, Map[String, Int]]]
        newLabelMap.empty
      } finally {
        modelLabelToVersionsWriteLock.unlock()
      }
    }
  }

  // Updates the base_path fields in each ModelConfig, prepending an absolute model_config_list_root_dir.
  // It is assumed that initially, all the base_path fields are relative.
  def updateModelConfigListRelativePaths(modelConfigListRootDir:String, modelConfigList: ModelConfigList)= {
    val updatedPaths = List[String]()
    for ((modelConfig: ModelConfig) <- modelConfigList.getConfigList){
      val basePath = modelConfig.getModelPlatform
      // Don't modify absolute paths.
      if (!uriIsRelativePath(basePath)){
        updatedPaths.:+ (basePath)
      } else {
        updatedPaths.:+(FilenameUtils.concat(modelConfigListRootDir, basePath))
        if (uriIsRelativePath(updatedPaths.last)){
          throw InvalidArguments(s"Expected model ${modelConfig.getName}, with updated base_path = " +
            s"JoinPath(${modelConfigListRootDir}, ${basePath}) to have an absolute path; got ${updatedPaths.last}")
        }
      }
    }

    (0 until updatedPaths.size).foreach{ i =>
      modelConfigList.getConfig(i).toBuilder.setBasePath(updatedPaths(i))
    }
  }

  def maybeUpdateServerRequestLogger(configCase: ConfigCase): Unit = ???

  def addModelsViaModelConfigList()= {
    val isFirstConfig = storagePathSourceAndRouter == None
    val sourceConfig = createStoragePathSourceConfig(config_)
    val routes = createStoragePathRoutes(config_)

    if (isFirstConfig){
      val adapters = createAdapters()
      val router = createRouter(routes, adapters)
      val source = createStoragePathSource(sourceConfig, router)

      // Connect the adapters to the manager, and wait for the models to load.
      connectAdaptersToManagerAndAwaitModelLoads(adapters)

      //store the source components
      storagePathSourceAndRouter = StoragePathSourceAndRouter(source, router)

    } else {
      // Create a fresh servable state monitor, to avoid getting confused if we're
      // re-loading a model-version that has previously been unloaded.
      val freshServableStateMonitor: ServableStateMonitor = ???


      // Figure out which models are new.
      val newModels = newModelNamesInSourceConfig(storagePathSourceAndRouter.source.config_, sourceConfig)

      // Now we're ready to start reconfiguring the elements of the Source->
      // Manager pipeline ...

      val oldAndNewRoutes = unionRoutes(storagePathSourceAndRouter.router.getRoutes, routes)
      reloadRoutes(oldAndNewRoutes)
      reloadStoragePathSourceConfig(sourceConfig)
      reloadRoutes(routes)
      waitUntilModelsAvailable(newModels, freshServableStateMonitor)//todo: ServableStateMonitor
    }
  }

  def addModelsViaCustomModelConfig()= {
    if (options_.customModelConfigLoader == None){
      throw InvalidArguments("Missing custom_model_config_loader in ServerCore Options")
    }
    manager_ = options_.customModelConfigLoader(config_.getCustomModelConfig, servableEventBus)
  }

  def connectAdaptersToManagerAndAwaitModelLoads(adapters: SourceAdapters): Unit = {
    val modelsToAwait = List[ServableRequest]()
    for ((modelConfig: ModelConfig) <- config_.getModelConfigList.getConfigList){
      modelsToAwait.:+(ServableRequest.earliest(modelConfig.getName))
    }
    val adapterList = List[Source[Loader]]()
    adapters.platformAdapters.foreach{ case(_, adapter) =>
      adapterList.:+(adapter)
    }
    adapterList :+(adapters.errorAdapter)
    LoadServablesFast.connectSourcesWithFastInitialLoad(manager_, adapterList, servableStateMonitor,
      modelsToAwait, options_.numInitialLoadThreads)
  }


  def newModelNamesInSourceConfig(oldConfig: FileSystemStoragePathSourceConfig,
                                  newConfig: FileSystemStoragePathSourceConfig): Set[String] = {
    val oldModels: Set[String] = Set[String]()
    for ((servable: ServableToMonitor) <- oldConfig.getServablesList){
      oldModels.+(servable.getServableName)
    }

    val newModels: Set[String] = Set[String]()
    for ((servable: ServableToMonitor) <- newConfig.getServablesList){
      val modelName = servable.getServableName
      if (!oldModels.contains(modelName)){
        newModels.+(modelName)
      }
    }
    newModels
  }

  def unionRoutes(routes1: Routes, routes2: Routes): Routes = {
    var result: Routes = routes1
    routes2.foreach{ case (platform, port) =>
      val port1 = routes1.get(platform)
      if (port1 == None){
        result += (platform -> port)
      } else {
        if (port1 != port){
          throw InvalidArguments("Conflict while unioning two route maps.")
        }
      }
    }
    result
  }

  def reloadRoutes(routes:Routes): Unit = {
    try{
      storagePathSourceAndRouter.router.updateRoutes(routes)
    } catch {
      case e: Exception =>{
        LOG(e)
      }
    }
  }

  def reloadStoragePathSourceConfig(sourceConfig: FileSystemStoragePathSourceConfig): Unit = {
    try{
      storagePathSourceAndRouter.source.updateConfig(sourceConfig)
    } catch {
      case e: Exception =>{
        LOG(e)
      }
    }
  }

  def waitUntilModelsAvailable(models: Set[String], monitor: ServableStateMonitor): Unit = ???
}
