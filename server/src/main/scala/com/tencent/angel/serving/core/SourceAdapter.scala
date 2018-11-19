package com.tencent.angel.serving.core

import java.util.concurrent.locks.{Condition, ReentrantLock}

abstract class SourceAdapter[S, T] extends TargetBase[T] with Source[S] {
  protected var outgoingCallback: AspiredVersionsCallback[S] = _
  protected var flag: Boolean = false
  val lock: ReentrantLock = new ReentrantLock
  val cond: Condition = lock.newCondition()

  def adapt(servableName: String, versions: List[ServableData[T]]): List[ServableData[S]]

  def adaptOneVersion(version: ServableData[T]): ServableData[S] = {
    adapt(version.id.name, List[ServableData[T]](version)).head
  }

  def setAspiredVersionsCallback(callback: AspiredVersionsCallback[S]): Unit = {
    lock.lock()
    try{
      outgoingCallback = callback
      flag = true
      cond.signal()
    } finally {
      lock.unlock()
    }
  }

  def setAspiredVersions(servableName: String, versions: List[ServableData[T]]): Unit = {
    lock.lock()
    try {
      while (!flag) {
        cond.await()
      }

      outgoingCallback(servableName, adapt(servableName, versions))
    } finally {
      lock.unlock()
    }
  }
}


abstract class UnarySourceAdapter[S, T] extends SourceAdapter[S, T] {
  def convert(data: ServableData[T]): ServableData[S]

  override def adapt(servableName: String, versions: List[ServableData[T]]): List[ServableData[S]] = {
    versions.map{version =>
      if (version.status == Status.OK) {
        val adapted = convert(version)
        if (adapted != null) {
          adapted
        } else {
          new ServableData[S](version.id, version.status, null.asInstanceOf[S])
        }
      } else {
        new ServableData[S](version.id, version.status, null.asInstanceOf[S])
      }
    }
  }
}


class ErrorSourceAdapter[S, T] extends SourceAdapter[S, T] {
  override def adapt(servableName: String, versions: List[ServableData[T]]): List[ServableData[S]] = {
    versions.map{version =>
      if (version.status == Status.OK) {
          new ServableData[S](version.id, Status.Error, null.asInstanceOf[S])
      } else {
        new ServableData[S](version.id, version.status, null.asInstanceOf[S])
      }
    }
  }
}


class IdentitySourceAdapter[T] extends SourceAdapter[T, T] {
  override def adapt(servableName: String, versions: List[ServableData[T]]): List[ServableData[T]] = versions
}

