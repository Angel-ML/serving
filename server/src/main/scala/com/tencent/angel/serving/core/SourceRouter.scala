package com.tencent.angel.serving.core

import java.util.concurrent.locks.ReentrantReadWriteLock

abstract class SourceRouter[T](val numOutputPorts: Int) extends TargetBase[T] {
  private var outputPorts: List[IdentitySourceAdapter[T]] = _

  def getOutputPorts: List[Source[T]] = synchronized(this) {
    if (outputPorts == null) {
      outputPorts = (0 until numOutputPorts).toList.map(_ => new IdentitySourceAdapter[T]())
      outputPorts
    } else {
      outputPorts
    }
  }

  def route(servableName: String, versions: List[ServableData[T]]): Int

  override def setAspiredVersions(servableName: String, versions: List[ServableData[T]]): Unit = {
    val outputPort = route(servableName, versions)
    if (outputPort < 0 || outputPort >= numOutputPorts) {
      throw RouteExceptions("route error!")
    }
    if (outputPorts == null) {
      getOutputPorts
    }

    outputPorts(outputPort).setAspiredVersions(servableName, versions)
  }
}

class DynamicSourceRouter[T] private(numOutputPorts: Int, private var routes: Routes) extends SourceRouter[T](numOutputPorts) {
  val lock = new ReentrantReadWriteLock()
  val readLock: ReentrantReadWriteLock.ReadLock = lock.readLock()
  val writeLock: ReentrantReadWriteLock.WriteLock = lock.writeLock()

  def getRoutes: Routes = {
    readLock.lock()
    try {
      routes
    } finally {
      readLock.unlock()
    }
  }

  def updateRoutes(newRoutes: Routes): Unit = {
    writeLock.lock()
    try {
      routes = newRoutes
    } finally {
      writeLock.unlock()
    }
  }

  override def route(servableName: String, versions: List[ServableData[T]]): Int = {
    readLock.lock()

    try {
      if (routes.contains(servableName)) {
        routes(servableName)
      } else {
        numOutputPorts - 1
      }
    } finally {
      readLock.unlock()
    }
  }
}

object DynamicSourceRouter {
  def apply[T](numOutputPorts: Int, routes: Routes): DynamicSourceRouter[T] = {
    assert(numOutputPorts == routes.size)
    new DynamicSourceRouter[T](numOutputPorts, routes)
  }
}
