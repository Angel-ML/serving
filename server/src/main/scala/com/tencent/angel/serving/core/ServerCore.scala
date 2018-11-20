package com.tencent.angel.serving.core

import com.tencent.angel.config.FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableToMonitor
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
  val options:ServerOptions

  case class SourceAdapters(platformAdapters: Map[String, StoragePathSourceAdapter],
                            errorAdapter: StoragePathSourceAdapter)

  def apply(options: ServerOptions): ServerCore = new ServerCore(options)

  def createAspiredVersionsManager(policy: AspiredVersionPolicy): AspiredVersionsManager = ???

  def createResourceTracker(): ResourceTracker = ???
  def createAdapter(modelPlatform: String): StoragePathSourceAdapter = ???

  def createStoragePathSourceConfig(config: ModelServerConfig): FileSystemStoragePathSourceConfig = {
    val source_config: FileSystemStoragePathSourceConfig = new FileSystemStoragePathSourceConfig()
//    source_config.toBuilder.setFileSystemPollWaitSeconds(options)//todo:options.getFileSystemPollWaitSeconds
//    source_config.toBuilder.setFailIfZeroVersionsAtStartup()
    for (model <- config.getModelConfigList.getConfigList){
      val servable: ServableToMonitor = source_config
    }

    source_config
  }

  def createStoragePathRoutes(config: ModelServerConfig): Routes = ???
  def createStoragePathSource(config: FileSystemStoragePathSourceConfig,
                              target: Target[StoragePath]): FileSystemStoragePathSource = ???
  def createRouter(routes: Routes, tragets: SourceAdapters): DynamicSourceRouter[StoragePath] = ???
  def createAdapters(): SourceAdapters = ???
}
