package com.tencent.angel.serving.core

trait Target[T] {
  def getAspiredVersionsCallback: AspiredVersionsCallback[T]
}


abstract class TargetBase[T] extends Target[T] {
  protected var detached: Boolean = false

  def setAspiredVersions(servableName: String, versions: List[ServableData[T]]): Unit

  def detach(): Unit = synchronized(detached) {
    if (!detached) {
      detached = true
    } else {
      throw new Exception("detach() must be called exactly once")
    }
  }

  def getAspiredVersionsCallback: AspiredVersionsCallback[T] = synchronized(detached) {
    if (detached) {
      (name: String, versions: List[ServableData[T]]) => {}
    } else {
      (name: String, versions: List[ServableData[T]]) => {
        setAspiredVersions(name, versions)
      }
    }
  }
}


object ConnectSourceToTarget {
  def apply[T](source: Source[T], target: Target[T]): Unit = {
    source.setAspiredVersionsCallback(target.getAspiredVersionsCallback)
  }
}
