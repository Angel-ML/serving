package com.tencent.angel.serving.core

trait Target[T] {
  def getAspiredVersionsCallback: AspiredVersionsCallback[T]
}


abstract class TargetBase[T] extends Target[T] {
  protected var detached: Boolean = false

  def setAspiredVersions(servableName: String, versions: List[ServableData[T]]): Unit

  def detach(): Unit = {
    if (!detached) {
      detached = true
    } else {
      throw new Exception("detach() must be called exactly once")
    }
  }

  def getAspiredVersionsCallback: AspiredVersionsCallback[T] = {
    if (detached) {
      (name: String, versions: List[ServableData[T]]) => {}
    } else {
      (name: String, versions: List[ServableData[T]]) => {
        setAspiredVersions(name, versions)
      }
    }
  }
}
