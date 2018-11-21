package com.tencent.angel.serving.core

import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}

import collection.mutable
import java.util.concurrent.{ExecutorService, Executors}

import com.tencent.angel.serving.core.LoadOrUnloadRequest.Kind
import com.tencent.angel.serving.core.ManagerState.ManagerState
import com.tencent.angel.serving.core.ServableStateMonitor.ServableStateAndTime
import org.slf4j.{Logger, LoggerFactory}


class BasicManager(numLoadThreads: Int, numUnloadThreads: Int,
                   maxNumLoadRetries: Int, loadRetryIntervalMicros: Long,
                   resourceRracker: ResourceTracker, servableEventBus: EventBus[ServableStateAndTime]) extends Manager {

  import BasicManager._

  val LOG: Logger = LoggerFactory.getLogger(classOf[BasicManager])

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

  def setAspiredState(id: ServableId, aspired: Boolean): Unit = {
    managedLock.lock()
    try {
      val harnessOpt = getHarnessFromManagedMap(id)
      if (harnessOpt.nonEmpty) {
        harnessOpt.get.setAspired(aspired)
      } else {
        LOG.info(s"Cannot find ServableId: ${id.toString}!")
      }
    } finally {
      managedLock.unlock()
    }
  }

  def manageServable(servable: ServableData[Loader]): Unit = {
    manageServableWithAdditionalState(servable, aspired = true)
  }

  def manageServableWithAdditionalState(servable: ServableData[Loader], aspired: Boolean): Unit = {
    val harness = LoaderHarness(servable.id, servable.data, maxNumLoadRetries, loadRetryIntervalMicros)
    harness.setAspired(aspired)

    managedLock.lock()
    try {
      val id = servable.id
      val harnessOpt = managedMap(id.name).find(harness => harness.id == id)
      if (harnessOpt.nonEmpty) {
        managedMap.addBinding(servable.id.name, harness)
        publishOnEventBus(new ServableState(servable.id, ManagerState.kStart))
      } else {
        LOG.info(s"This servable is already being managed: ${id.toString}")
      }
    } finally {
      managedLock.unlock()
    }

  }

  def stopManagingServable(id: ServableId): Unit = {
    managedLock.lock()
    try {
      val harnessOpt = getHarnessFromManagedMap(id)
      if (harnessOpt.nonEmpty) {
        val harness = harnessOpt.get
        harness.state match {
          case LoaderHarness.State.kNew | LoaderHarness.State.kDisabled | LoaderHarness.State.kError =>
            managedMap.removeBinding(id.name, harness)
          case _ => LOG.info("The state of harness is not kNew, kDisabled or kError, maybe in unloading!")
        }
      } else {
        LOG.info("Cannot find the stop version!")
      }
    } finally {
      managedLock.unlock()
    }
  }

  private def getHarnessFromManagedMap(id: ServableId): Option[LoaderHarness] = {
    managedMap(id.name).find { harness => harness.id == id }
  }

  //---------------------------------------------------------------------------Load/UnloadActions
  def loadServable(id: ServableId): Unit = {
    val req = LoadOrUnloadRequest(id, Kind.kLoad)
    loadOrUnloadServable(req)
  }

  def unloadServable(id: ServableId): Unit = {
    val req = LoadOrUnloadRequest(id, Kind.kUnload)
    loadOrUnloadServable(req)
  }

  def loadOrUnloadServable(request: LoadOrUnloadRequest): Unit = {
    val harnessOpt = getHarnessFromManagedMap(request.servableId)
    if (harnessOpt.nonEmpty) {
      val harness = harnessOpt.get
      request.kind match {
        case Kind.kUnload =>
          harness.unloadRequested()
          unloadExecutor.submit(new Runnable {
            override def run(): Unit = {
              handleLoadOrUnloadRequest(request)
            }
          })
        case Kind.kLoad =>
          harness.loadRequested()
          loadExecutor.submit(new Runnable {
            override def run(): Unit = {
              handleLoadOrUnloadRequest(request)
            }
          })
      }
    }
  }

  def handleLoadOrUnloadRequest(request: LoadOrUnloadRequest): Unit = {
    val harnessOpt = getHarnessFromManagedMap(request.servableId)
    if (harnessOpt.nonEmpty) {
      val harness = harnessOpt.get
      val approved = approveLoadOrUnload(request, harness)

      if (approved) {
        executeLoadOrUnload(request, harness)
      } else {
        // not approved
      }
    } else {
      // harness not find
    }
  }

  def approveLoadOrUnload(request: LoadOrUnloadRequest, harness: LoaderHarness): Boolean = {
    request.kind match {
      case Kind.kUnload =>
        approveLoad(harness)
      case Kind.kLoad =>
        approveUnload(harness)
    }
  }

  def approveLoad(harness: LoaderHarness): Boolean = {
    val resourceReservationStatus = reserveResources(harness)

    if (!resourceReservationStatus._1) {
      harness.error(resourceReservationStatus._2)
      publishOnEventBus(new ServableState(harness.id, ManagerState.kEnd))
    } else {
      harness.loadApproved()
    }

    resourceReservationStatus._1
  }

  def approveUnload(harness: LoaderHarness): Boolean = {
    harness.startQuiescing()
    true
  }

  def executeLoadOrUnload(request: LoadOrUnloadRequest, harness: LoaderHarness): Unit = {
    request.kind match {
      case Kind.kUnload =>
        executeLoad(harness)
      case Kind.kLoad =>
        executeUnload(harness)
    }
  }

  def executeLoad(harness: LoaderHarness): Unit = {
    publishOnEventBus(new ServableState(harness.id, ManagerState.kLoading))
    harness.load()
    servingMap.update(managedMap)
    publishOnEventBus(new ServableState(harness.id, ManagerState.kAvailable))
  }

  def executeUnload(harness: LoaderHarness): Unit = {
    publishOnEventBus(new ServableState(harness.id, ManagerState.kUnloading))
    servingMap.update(managedMap)
    harness.doneQuiescing()
    harness.unload()
    publishOnEventBus(new ServableState(harness.id, ManagerState.kEnd))
  }

  def cancelLoadServableRetry(id: ServableId)

  def reserveResources(harness: LoaderHarness): (Boolean, ManagerState)

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
