package com.tencent.angel.serving


package object core {
  type Routes = Map[String, Int]

  type StoragePath = String

  type AspiredVersionsCallback[T] = (String, List[ServableData[T]]) => Unit

  type StoragePathSourceAdapter = SourceAdapter[StoragePath, Loader]

  type CustomModelConfigLoader = (Any, EventBus[ServableState], AspiredVersionsManager) => Unit

  type ServableStateMonitorCreator = (EventBus[ServableState], ServableStateMonitor) => Unit
}
