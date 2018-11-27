package com.tencent.angel.serving.core

import java.net.URI
import java.util.concurrent.locks.ReentrantLock

import com.tencent.angel.config.ModelServerConfigProtos.{ModelConfig, ModelConfigList, ModelServerConfig}
import com.tencent.angel.serving.core.ServableStateMonitor.VersionMap
import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import org.apache.commons.io.FilenameUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable



class ServerCore(val context: CoreContext) extends Manager {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[ServerCore])

  private val servableStateMonitor = context.monitor
  private val aspiredVersionPolicy: AspiredVersionPolicy = context.aspiredVersionPolicy
  private val manager: AspiredVersionsManager = new AspiredVersionsManager(
    context.manageStateDelayMicros, context.manageStateIntervalMicros, aspiredVersionPolicy,
    context.numLoadThreads, context.numUnloadThreads, context.maxNumLoadRetries, context.loadRetryIntervalMicros,
    context.totalResources, context.eventBus)

  private val modelLabel2VersionsLock = new ReentrantLock()
  private var modelLabels2Versions = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

  private val configLock = new ReentrantLock()
  private var config: ModelServerConfig = _

  context.manager = manager

  //-------------------------------------------------------------------------Server Setup and Initialization
  def reloadConfig(newConfig: ModelServerConfig): Unit = {
    configLock.lock()
    try {
      // Determine whether to accept this config transition.
      val isFirstConfig = config == null || config.getConfigCase == ModelServerConfig.ConfigCase.CONFIG_NOT_SET
      val acceptTransition = isFirstConfig || (config.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST
        && newConfig.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST)
      if (!acceptTransition) {
        throw FailedPreconditions("Cannot transition to requested config. It is only legal to transition " +
          "from one ModelConfigList to another.")
      }
      if (newConfig.getConfigCase == ModelServerConfig.ConfigCase.CONFIG_NOT_SET) {
        //Nothing to load. In this case we allow a future call with a non-empty config.
        LOG.info("nothing to load, taking no action fo empty config")
        return
      }
      if (newConfig.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST &&
        config.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST) {
        validateNoModelsChangePlatforms(config.getModelConfigList, newConfig.getModelConfigList)
      }
      if (newConfig.getConfigCase == ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST) {
        config = validateModelConfigList(newConfig)
      } else {
        config = newConfig
      }
      updateModelVersionLabelMap()

      LOG.info("adding or updating models")
      config.getConfigCase match {
        case ModelServerConfig.ConfigCase.MODEL_CONFIG_LIST =>
          context.addModelsViaModelConfigList(config)
        case ModelServerConfig.ConfigCase.CUSTOM_MODEL_CONFIG =>
          if (context.customModelConfigLoader == null) {
            throw InvalidArguments("Missing custom_model_config_loader in ServerCore Options")
          } else {
            context.customModelConfigLoader(config.getCustomModelConfig, manager)
          }
        case _ => throw new Exception("Invalid ServerModelConfig")
      }
      context.maybeUpdateServerRequestLogger(config.getConfigCase)

    } finally {
      configLock.unlock()
    }
  }

  // make sure: no replicated names, and no relative path
  private def validateModelConfigList(config: ModelServerConfig): ModelServerConfig = {
    //unique model
    val modelNames = new mutable.HashSet[String]()
    config.getModelConfigList.getConfigList.asScala.foreach { model => // ModelConfig
      val name = model.getName
      if (modelNames.contains(name)) {
        throw InvalidArguments(s"Illegal to list model $name, multiple times in config list")
      }

      if (ServerCore.uriIsRelativePath(model.getBasePath)) {
        throw InvalidArguments(s"Expected model ${model.getName} to have an absolute path or URI; " +
          s"got base_path()=${model.getBasePath}")
      }
      modelNames.add(name)
    }

    // Base-paths are either all relative, or all absolute.
    // WARNING: abuse of terminology!  These "paths" may be URIs
    if (context.modelConfigListRootDir != "") {
      //all base path must be relative
      if (ServerCore.uriIsRelativePath(context.modelConfigListRootDir)) {
        //todo
        throw InvalidArguments(s"Expected non-empty absolute path or URI; " +
          s"got model_config_list_root_dir= ${context.modelConfigListRootDir}")
      } else {
        return updateModelConfigListRelativePaths(context.modelConfigListRootDir, config)
      }
    } else {
      // all base path must be absolute
      config.getModelConfigList.getConfigList.asScala.foreach{ modelConfig =>
        if (ServerCore.uriIsRelativePath(modelConfig.getBasePath)){
          throw InvalidArguments(s"Expected model: ${modelConfig.getName} " +
            s" to have an absolute path or uri, basepath = ${modelConfig.getBasePath}")
        }
      }
    }
    config
  }

  private def validateNoModelsChangePlatforms(oldConfigList: ModelConfigList, newConfigList: ModelConfigList): Unit = {
    val oldModelPlatforms = oldConfigList.getConfigList.asScala.toList.map { oldConfig =>
      oldConfig.getName -> ServerCore.getPlatform(oldConfig)
    }.toMap

    newConfigList.getConfigList.asScala.foreach { newConfig =>
      val oldPlatform = oldModelPlatforms.get(newConfig.getName)
      if (oldPlatform.nonEmpty) {
        val newPlatform = ServerCore.getPlatform(newConfig)
        if (oldPlatform.get != newPlatform) {
          throw InvalidArguments(s"Illegal to change a model's platform. For model ${newConfig.getName} platform was " +
            s"${oldPlatform}old_platform, and new platform requested is $newPlatform")
        }
      }
    }
  }

  private def updateModelConfigListRelativePaths(modelConfigListRootDir: String,
                                                 config: ModelServerConfig): ModelServerConfig = {
    val configBuilder: ModelServerConfig.Builder = config.toBuilder
    val builder: ModelConfigList.Builder = configBuilder.getModelConfigListBuilder

    config.getModelConfigList.getConfigList.asScala.zipWithIndex.foreach { case (modelConfig, idx) =>
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
    configBuilder.build()
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
  @throws(classOf[CoreExceptions])
  def servableRequestFromModelSpec(modelSpec: ModelSpec): ServableRequest = {
    val name = modelSpec.getName
    if (name.isEmpty) {
      throw CoreExceptions("modelSpec name is empty")
    }

    modelSpec.getVersionChoiceCase match {
      case ModelSpec.VersionChoiceCase.VERSION =>
        ServableRequest.specific(name, modelSpec.getVersion.getValue)
      case ModelSpec.VersionChoiceCase.VERSION_LABEL =>
        val version = getModelVersionForLabel(name, modelSpec.getVersionLabel)
        ServableRequest.specific(name, version)
      case ModelSpec.VersionChoiceCase.VERSIONCHOICE_NOT_SET =>
        ServableRequest.latest(name)
    }
  }

  @throws(classOf[CoreExceptions])
  def getModelVersionForLabel(modelName: String, label: String): Long = {
    try {
      modelLabels2Versions(modelName)(label)
    } catch {
      case _: Exception => throw CoreExceptions("version not found!")
    }
  }

  def updateModelVersionLabelMap(): Unit = {
    // modelName --> label --> version
    val newLabelMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()
    config.getModelConfigList.getConfigList.asScala.foreach { modelConfig =>
      val servingStates: VersionMap = servableStateMonitor.getVersionStates(modelConfig.getName)

      modelConfig.getVersionLabelsMap.asScala.foreach { case (label, version) =>
        //Verify that the label points to a version that is currently available.
        val servableStateAndTime = servingStates(version)
        if (servableStateAndTime == null || servableStateAndTime.state.managerState != ManagerState.kAvailable) {
          throw FailedPreconditions(s"Request to assign label to $version, version of model ${modelConfig.getName}, " +
            s"which is not currently available for inference")
        }

        if (newLabelMap.contains(modelConfig.getName)) {
          newLabelMap(modelConfig.getName)(label) = version
        } else {
          newLabelMap(modelConfig.getName) = mutable.HashMap[String, Long](label -> version)
        }
      }
    }

    if (!context.allowVersionLabels) {
      if (newLabelMap.nonEmpty) {
        throw FailedPreconditions("Model version labels are not currently allowed by the server.")
      }
    } else {
      modelLabel2VersionsLock.lock()
      try {
        modelLabels2Versions = newLabelMap
      } finally {
        modelLabel2VersionsLock.unlock()
      }
    }
  }
}


object ServerCore {

  def apply(ctx: CoreContext): ServerCore = new ServerCore(ctx)

  case class SourceAdapters(platformAdapters: Map[String, StoragePathSourceAdapter],
                            errorAdapter: StoragePathSourceAdapter)


  def uriIsRelativePath(uriStr: String): Boolean = {
    val uri = new URI(uriStr)
    !uri.isAbsolute
  }

  def getPlatform(modelConfig: ModelConfig): String = {
    val platform: String = modelConfig.getModelPlatform
    if (platform == "") {
      throw InvalidArguments(s"Illegal setting ModelServerConfig::model_platform.")
    }
    platform
  }

}
