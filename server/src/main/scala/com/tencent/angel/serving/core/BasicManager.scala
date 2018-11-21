package com.tencent.angel.serving.core

import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}

import collection.mutable
import java.util.concurrent.{ExecutorService, Executors}

import com.tencent.angel.serving.core.ServableStateMonitor.ServableStateAndTime


class BasicManager(numLoadThreads: Int, numUnloadThreads: Int,
                   maxNumLoadRetries: Int, loadRetryIntervalMicros: Long,
                   resourceRracker: ResourceTracker, servableEventBus: EventBus[ServableStateAndTime]) extends Manager {

  import BasicManager._

  private val loadExecutor: ExecutorService = Executors.newFixedThreadPool(numLoadThreads)
  private val unloadExecutor: ExecutorService = Executors.newFixedThreadPool(numUnloadThreads)

  private val managedLock = new ReentrantLock()
  private val managedMap: ManagedMap = new mutable.HashMap[String, mutable.Set[LoaderHarness]]
    with mutable.MultiMap[String, LoaderHarness].asInstanceOf[mutable.MultiMap[String, LoaderHarness]]

  val servingMap: ServingMap = new ServingMap

  //---------------------------------------------------------------------------ManagedMap
  def getManagedServableStateSnapshots(servableName: String): List[ServableStateSnapshot[Aspired]] = {
    managedLock.lock()
    try {
      managedMap(servableName).toList.map { harness => harness.loaderStateSnapshot() }
    } finally {
      managedLock.unlock()
    }
  }

  def getManagedServableNames: List[String] = {
    managedLock.lock()
    try {
      managedMap.keySet.toList
    } finally {
      managedLock.unlock()
    }
  }

  def getAdditionalServableState(id: ServableId): Aspired = {
    managedLock.lock()
    try {
      val harness = managedMap(id.name).find{ harness => harness.id.version == id.version }
      harness.get.additionalState
    } finally {
      managedLock.unlock()
    }
  }

  def manageServable(servable: ServableData[Loader])

  def manageServableWithAdditionalState(servable: ServableData[Loader], aspired: Boolean)

  def stopManagingServable(id: ServableId)

  //---------------------------------------------------------------------------Load/UnloadActions
  def loadServable(id: ServableId)

  def cancelLoadServableRetry(id: ServableId)

  def unloadServable(id: ServableId)

  //---------------------------------------------------------------------------Manager
  override def availableServableIds: List[ServableId] = servingMap.availableServableIds

  override def availableServableHandles[T]: Map[ServableId, ServableHandle[T]] = ???

  override def servableHandle[T](request: ServableRequest): ServableHandle[T] = ???

  override def untypedServableHandle(request: ServableRequest): UntypedServableHandle = {
    servingMap.untypedServableHandle(request)
  }

  override def availableUntypedServableHandles: Map[ServableId, UntypedServableHandle] = {
    servingMap.availableUntypedServableHandles
  }

  //---------------------------------------------------------------------------
  def publishOnEventBus(state: ServableState): Unit = {
    if (servableEventBus != null) {
      servableEventBus.publish(state)
    }
  }
}

object BasicManager {
  type ManagedMap = mutable.MultiMap[String, LoaderHarness]

  type HandlesMap = mutable.MultiMap[ServableRequest, LoaderHarness]

  class ServingMap {
    private val lock = new ReentrantReadWriteLock()
    private val readLock = lock.readLock()
    private val writeLock = lock.writeLock()
    private val handlesMap: HandlesMap = new mutable.HashMap[ServableRequest, mutable.Set[LoaderHarness]]
      with mutable.MultiMap[ServableRequest, LoaderHarness].asInstanceOf[mutable.MultiMap[ServableRequest, LoaderHarness]]

    def availableServableIds: List[ServableId] = {
      readLock.lock()
      try {

      } finally {
        readLock.unlock()
      }
    }

    def untypedServableHandle(request: ServableRequest): UntypedServableHandle = {
      readLock.lock()
      try {

      } finally {
        readLock.unlock()
      }
    }

    def availableUntypedServableHandles: Map[ServableId, UntypedServableHandle] = {
      readLock.lock()
      try {

      } finally {
        readLock.unlock()
      }
    }

    def update(managedMap: ManagedMap): Unit = {
      writeLock.lock()
      try {

      } finally {
        writeLock.unlock()
      }
    }
  }

}
