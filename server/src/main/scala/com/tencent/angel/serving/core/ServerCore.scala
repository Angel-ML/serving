package com.tencent.angel.serving.core

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
