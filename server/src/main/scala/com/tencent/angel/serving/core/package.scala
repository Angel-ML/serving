package com.tencent.angel.serving


package object core {
  type Routes = Map[String, Int]

  type StoragePath = String

  type AspiredVersionsCallback[T] = (String, List[ServableData[T]]) => Unit

  type StoragePathSourceAdapter = SourceAdapter[Loader, StoragePath]

  type CustomModelConfigLoader = (Any, AspiredVersionsManager) => Unit

  type ServableStateMonitorCreator = (EventBus[ServableState], ServableStateMonitor) => Unit
}
