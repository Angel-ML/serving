package com.tencent.angel.serving.core

import com.tencent.angel.config.FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig
import com.tencent.angel.config.FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableToMonitor
import com.tencent.angel.config.ModelServerConfigProtos.ModelServerConfig.ConfigCase
import com.tencent.angel.config.ModelServerConfigProtos.{ModelConfig, ModelConfigList, ModelServerConfig}
import com.tencent.angel.serving.core.ManagerState.ManagerState
import com.tencent.angel.serving.serving._
import com.tencent.angel.serving.sources.FileSystemStoragePathSource

case class ServerOptions()

class ServerCore(val options: ServerOptions) extends Manager {
  override def availableServableIds: List[ServableId] = ???

  override def availableServableHandles[T]: Map[ServableId, ServableHandle[T]] = ???

  override def servableHandle[T](request: ServableRequest): ServableHandle[T] = ???

  override def untypedServableHandle(request: ServableRequest): UntypedServableHandle = ???

  override def availableUntypedServableHandles: Map[ServableId, UntypedServableHandle] = ???
}

object ServerCore {
  var options:ServerOptions = ???
  var platformToRouterPort: Map[String, Int] = ???
  var config_ : ModelServerConfig = ???
  var storagePathSourceAndRouter: StoragePathSourceAndRouter = null
  var manager_ : AspiredVersionsManager = ???

  case class StoragePathSourceAndRouter(source: FileSystemStoragePathSource, router: DynamicSourceRouter[StoragePath])

  case class SourceAdapters(platformAdapters: Map[String, StoragePathSourceAdapter],
                            errorAdapter: StoragePathSourceAdapter)

  def apply(options: ServerOptions): ServerCore = new ServerCore(options)

  def createAspiredVersionsManager(policy: AspiredVersionPolicy): AspiredVersionsManager = ???

  def createResourceTracker(): ResourceTracker = ???

  def createAdapter(modelPlatform: String): StoragePathSourceAdapter = ???

  def createStoragePathSourceConfig(config: ModelServerConfig): FileSystemStoragePathSourceConfig = {
    val servables = new java.util.ArrayList[ServableToMonitor]()
    for ((model:ModelConfig) <- config.getModelConfigList.getConfigList){
      val monitorBuilder = ServableToMonitor.newBuilder()
      monitorBuilder.setServableName(model.getName)
      monitorBuilder.setBasePath(model.getBasePath)
      monitorBuilder.setServableVersionPolicy(model.getModelVersionPolicy)
      servables.add(new ServableToMonitor(monitorBuilder))
    }
    val builder = FileSystemStoragePathSourceConfig.newBuilder()
    builder.addAllServables(servables)
    builder.setFileSystemPollWaitSeconds(100)//todo:options.getFileSystemPollWaitSeconds
    builder.setFailIfZeroVersionsAtStartup(true)//todo:options.FailIfZeroVersionsAtStartup
    builder.build()
  }

  def createStoragePathRoutes(config: ModelServerConfig): Routes = {
    var routes:Routes = Map[String, Int]()
    for ((model: ModelConfig) <- config.getModelConfigList.getConfigList){
      val modelName: String = model.getName
      val platform: String = getPlatform(model)
      val port: Int = platformToRouterPort.get(platform).to[Int]
      if (port == None){
        throw new Exception(s"Model ${modelName},requests unsupported platform ${platform}")
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
      val port: Int = platformToRouterPort.get(platform).to[Int]
      if (port == None){
        throw new Exception("Router port for platform not found.")
      }
      ConnectSourceToTarget(outputPorts(port), adapter)
    }
    ConnectSourceToTarget(outputPorts.last, targets.errorAdapter)

    router
  }

  def createAdapters(): SourceAdapters = {
    var platformAdapters: Map[String, StoragePathSourceAdapter] = Map[String, StoragePathSourceAdapter]()
    platformToRouterPort.foreach{ case (platform, port) =>
        val adapter: StoragePathSourceAdapter = createAdapter(platform)
        platformAdapters += (platform -> adapter)
    }
    val errorAdapters = new ErrorSourceAdapter[StoragePath, Loader]()//todo:error
    SourceAdapters(platformAdapters, errorAdapters)
  }

  def getPlatform(modelConfig: ModelConfig): String = {
    val platform:String = modelConfig.getModelPlatform
    if (platform == ""){
      throw new Exception(s"Illegal setting ModelServerConfig::model_platform.")
    }
    platform
  }

  def reloadConfig(newConfig: ModelServerConfig): Unit ={
    //TODO:LOCK

    try {
      // Determine whether to accept this config transition.
      val isFirstConfig = config_.getConfigCase == ModelServerConfig.ConfigCase.CONFIG_NOT_SET
      val acceptTransition = isFirstConfig || (config_.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST
        && newConfig.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST)
      if (!acceptTransition){
        throw new Exception("Cannot transition to requested config. It is only legal to transition " +
          "from one ModelConfigList to another.")
      }
      if (newConfig.getConfigCase == ModelServerConfig.ConfigCase.CONFIG_NOT_SET){
        //TODO: log
        //Nothing to load. In this case we allow a future call with a non-empty config.
        return
      }
      if (newConfig.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST){
        options = validateModelConfigList(newConfig.getModelConfigList)
      }
      if(newConfig.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST &&
        config_.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST){
        validateNoModelsChangePlatforms(config_.getModelConfigList, newConfig.getModelConfigList)
      }
      config_ = newConfig
      updateModelVersionLabelMap()

      //adding or updating models
      config_.getConfigCase match {
        case ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST =>{
          if (true){//TODO: options_.model_config_list_root_dir

          }
          addModelsViaModelConfigList()
        }
        case ModelServerConfig.ConfigCase.CUSTOM_MODEL_CONFIG =>{
          addModelsViaCustomModelConfig()
        }
        case _ => throw new Exception("Invalid ServerModelConfig")
      }
      maybeUpdateServerRequestLogger(config_.getConfigCase)

      //TODO:options.flushFilEsystemCaches
    } finally {

    }

  }

  def validateModelConfigList(configList: ModelConfigList):ServerOptions = ???
  def validateNoModelsChangePlatforms(oldConfigList: ModelConfigList, newConfigList: ModelConfigList) = ???
  def updateModelVersionLabelMap()= ???
  def maybeUpdateServerRequestLogger(configCase: ConfigCase)= ???

  def addModelsViaModelConfigList()= {
    val isFirstConfig = storagePathSourceAndRouter == null
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


      // Figure out which models are new.
      val newModels = newModelNamesInSourceConfig(storagePathSourceAndRouter.source.config_, sourceConfig)

      // Now we're ready to start reconfiguring the elements of the Source->
      // Manager pipeline ...

      val oldAndNewRoutes = unionRoutes(storagePathSourceAndRouter.router.getRoutes, routes)
      reloadRoutes(oldAndNewRoutes)
      reloadStoragePathSourceConfig(sourceConfig)
      reloadRoutes(routes)
      waitUntilModelsAvailable(newModels, null)//todo: ServableStateMonitor
    }
  }

  def addModelsViaCustomModelConfig()= ???

  def connectAdaptersToManagerAndAwaitModelLoads(adapters: SourceAdapters)= ???

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
          throw new Exception("Conflict while unioning two route maps.")
        }
      }
    }
    result
  }

  def reloadRoutes(routes:Routes)= {
    try{
      storagePathSourceAndRouter.router.updateRoutes(routes)
    } catch {
      case e: Exception =>{
        //todo;
      }
    }
  }

  def reloadStoragePathSourceConfig(sourceConfig: FileSystemStoragePathSourceConfig)= {
    try{
      storagePathSourceAndRouter.source.updateConfig(sourceConfig)
    } catch {
      case e: Exception =>{
        //todo;
      }
    }
  }

  def waitUntilModelsAvailable(models: Set[String], monitor: ServableStateMonitor)= ???
}
