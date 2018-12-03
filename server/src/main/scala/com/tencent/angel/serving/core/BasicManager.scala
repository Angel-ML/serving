package com.tencent.angel.serving.core

import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}

import collection.mutable
import java.util.concurrent.{ExecutorService, Executors}

import com.tencent.angel.config.ResourceAllocation
import com.tencent.angel.serving.core.LoadOrUnloadRequest.Kind
import com.tencent.angel.serving.core.LoaderHarness.State
import com.tencent.angel.serving.core.ServableRequest.AutoVersionPolicy
import com.tencent.angel.serving.core.ServableStateMonitor.ServableStateAndTime
import org.slf4j.{Logger, LoggerFactory}


class BasicManager(var numLoadThreads: Int, var numUnloadThreads: Int,
                   maxNumLoadRetries: Int, loadRetryIntervalMicros: Long,
                   totalResources: ResourceAllocation,
                   servableEventBus: EventBus[ServableState]) extends Manager {

  import BasicManager._

  private val LOG: Logger = LoggerFactory.getLogger(classOf[BasicManager])

  val resourceRracker: ResourceTracker = new ResourceTracker(totalResources,
    maxNumLoadRetries, loadRetryIntervalMicros)

  private val executorLock = new ReentrantLock()
  private val loadExecutor: ExecutorService = Executors.newFixedThreadPool(numLoadThreads)
  private val unloadExecutor: ExecutorService = Executors.newFixedThreadPool(numUnloadThreads)

  val managedMap: ManagedMap = new ManagedMap(maxNumLoadRetries, loadRetryIntervalMicros)
  val servingMap: ServingMap = new ServingMap

  //---------------------------------------------------------------------------Load/UnloadActions
  def loadServable(id: ServableId): Unit = {
    val req = LoadOrUnloadRequest(id, Kind.kLoad)
    LOG.info(s"loadServable: ${id.toString}")
    loadOrUnloadServable(req)
  }

  def unloadServable(id: ServableId): Unit = {
    val req = LoadOrUnloadRequest(id, Kind.kUnload)
    LOG.info(s"unloadServable: ${id.toString}")
    loadOrUnloadServable(req)
  }

  private def loadOrUnloadServable(request: LoadOrUnloadRequest): Unit = {
    executorLock.lock()
    try {
      val harnessOpt = managedMap.getHarnessOption(request.servableId)
      LOG.info(s"request: <${request.servableId.toString}, ${request.kind}> is ready to send to thread!")
      if (harnessOpt.nonEmpty) {
        val harness = harnessOpt.get
        request.kind match {
          case Kind.kUnload =>
            harness.unloadRequested()
            LOG.info("[LoaderHarness-State] kReady --> kUnloadRequested")
            unloadExecutor.submit(new Runnable {
              override def run(): Unit = {
                handleLoadOrUnloadRequest(request)
              }
            })
          case Kind.kLoad =>
            harness.loadRequested()
            LOG.info("[LoaderHarness-State] kNew --> kLoadRequested")
            loadExecutor.submit(new Runnable {
              override def run(): Unit = {
                handleLoadOrUnloadRequest(request)
              }
            })
        }
      }
      
    } finally {
      executorLock.unlock()
    }
  }

  private def handleLoadOrUnloadRequest(request: LoadOrUnloadRequest): Unit = {
    val harnessOpt = managedMap.getHarnessOption(request.servableId)
    if (harnessOpt.nonEmpty) {
      val harness = harnessOpt.get
      LOG.info("approveLoadOrUnload")
      val approved = approveLoadOrUnload(request, harness)

      if (approved) {
        LOG.info("approved, Begin to execute")
        executeLoadOrUnload(request, harness)
      } else {
        // not approved
      }
    } else {
      // harness not find
    }
  }

  private def approveLoadOrUnload(request: LoadOrUnloadRequest, harness: LoaderHarness): Boolean = {
    request.kind match {
      case Kind.kUnload =>
        approveUnload(harness)
      case Kind.kLoad =>
        approveLoad(harness)
    }
  }

  private def approveLoad(harness: LoaderHarness): Boolean = {
    LOG.info("The frist step of approveLoad: reserveResources")
    val resourceReservationStatus = reserveResources(harness)

    LOG.info("The second step of approveLoad: update status in LoaderHarness")
    if (!resourceReservationStatus) {
      harness.error()
      publishOnEventBus(new ServableState(harness.id, ManagerState.kEnd))
      LOG.info("[Manager-State] kEnd because of error")
    } else {
      harness.loadApproved()
      LOG.info("[LoaderHarness-State] kLoadRequested --> kLoadApproved")
    }

    resourceReservationStatus
  }

  private def approveUnload(harness: LoaderHarness): Boolean = {
    harness.startQuiescing()
    true
  }

  private def executeLoadOrUnload(request: LoadOrUnloadRequest, harness: LoaderHarness): Unit = {
    request.kind match {
      case Kind.kUnload =>
        executeLoad(harness)
      case Kind.kLoad =>
        executeUnload(harness)
    }
  }

  private def executeLoad(harness: LoaderHarness): Unit = {
    publishOnEventBus(new ServableState(harness.id, ManagerState.kLoading))
    harness.load()
    servingMap.update(managedMap)
    publishOnEventBus(new ServableState(harness.id, ManagerState.kAvailable))
  }

  private def executeUnload(harness: LoaderHarness): Unit = {
    publishOnEventBus(new ServableState(harness.id, ManagerState.kUnloading))
    servingMap.update(managedMap)
    harness.doneQuiescing()
    harness.unload()
    publishOnEventBus(new ServableState(harness.id, ManagerState.kEnd))
  }

  def cancelLoadServableRetry(id: ServableId): Unit = {
    val harnessOpt = managedMap.getHarnessOption(id)
    if (harnessOpt.nonEmpty) {
      harnessOpt.get.cancelLoadRetry()
    }
  }

  private def reserveResources(harness: LoaderHarness): Boolean = this.synchronized {
    // GetLoadersCurrentlyUsingResources
    val harnessList = new mutable.ListBuffer[LoaderHarness]()
    LOG.info("[reserveResources] 1. get harnesses that has used resources")
    managedMap.getManagedServableNames.foreach(name =>
      managedMap.getManagedLoaderHarness(name).foreach { harness =>
        val usesResources = harness.state match {
          case State.kNew => false
          case State.kLoadRequested => false
          case State.kLoadApproved => false
          case State.kLoading => true
          case State.kReady => true
          case State.kQuiescing => true
          case State.kQuiesced => true
          case State.kUnloadRequested => true
          case State.kUnloading => true
          case State.kDisabled => false
          case State.kError => false
        }

        if (usesResources) {
          harnessList.append(harness)
        }
      }
    )

    LOG.info("[reserveResources] 2. recomputeUsedResources")
    resourceRracker.recomputeUsedResources(harnessList.toList)
    LOG.info("[reserveResources] 3. reserveResources")
    resourceRracker.reserveResources(harness)
  }

  //---------------------------------------------------------------------------ManagedMap
  def getManagedServableStateSnapshots(servableName: String): List[ServableStateSnapshot] = {
    managedMap.getManagedServableStateSnapshots(servableName)
  }

  def getManagedServableNames: List[String] = {
    managedMap.getManagedServableNames
  }

  def setAspiredState(id: ServableId, aspired: Boolean): Unit = {
    managedMap.setAspiredState(id, aspired)
  }

  def manageServable(servable: ServableData[Loader]): Unit = {
    managedMap.manageServableWithAdditionalState(servable, aspired = true)_
  }

  def manageServableWithAdditionalState(servable: ServableData[Loader], aspired: Boolean): Unit = {
    managedMap.manageServableWithAdditionalState(servable, aspired) {
      state: ServableState => publishOnEventBus(state)
    }
  }

  def stopManagingServable(id: ServableId): Unit = {
    managedMap.stopManagingServable(id)
  }

  //---------------------------------------------------------------------------Manager
  override def availableServableIds: List[ServableId] = servingMap.availableServableIds

  override def availableServableHandles[T]: Map[ServableId, ServableHandle[T]] = {
    servingMap.availableUntypedServableHandles.map {
      case (id, untyped) => id -> new ServableHandle[T](untyped)
    }
  }

  override def servableHandle[T](request: ServableRequest): ServableHandle[T] = {
    new ServableHandle[T](servingMap.untypedServableHandle(request))
  }

  override def untypedServableHandle(request: ServableRequest): UntypedServableHandle = {
    servingMap.untypedServableHandle(request)
  }

  override def availableUntypedServableHandles: Map[ServableId, UntypedServableHandle] = {
    servingMap.availableUntypedServableHandles
  }

  //---------------------------------------------------------------------------
  private def publishOnEventBus(state: ServableState): Unit = {
    if (servableEventBus != null) {
      servableEventBus.publish(state)
    }
  }
}

object BasicManager {

  class ManagedMap(maxNumLoadRetries: Int, loadRetryIntervalMicros: Long) {
    type ManageMap = mutable.MultiMap[String, LoaderHarness]

    private val LOG: Logger = LoggerFactory.getLogger(classOf[BasicManager])

    val managedLock = new ReentrantLock()
    private val managedMap: ManageMap = new mutable.HashMap[String, mutable.Set[LoaderHarness]]
      with mutable.MultiMap[String, LoaderHarness].asInstanceOf[mutable.MultiMap[String, LoaderHarness]]

    def getManagedServableStateSnapshots(servableName: String): List[ServableStateSnapshot] = {
      managedLock.lock()
      try {
        val harnessLoader = managedMap.getOrElse(servableName, null)
        if (harnessLoader != null){
          harnessLoader.toList.map { harness => harness.loaderStateSnapshot() }
        } else {
          List()
        }
      } finally {
        managedLock.unlock()
      }
    }

    def getManagedLoaderHarness(servableName: String): List[LoaderHarness] = {
      managedLock.lock()
      try {
        managedMap(servableName).toList
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
        val harnessOpt = getHarnessInternal(id)
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
      manageServableWithAdditionalState(servable, aspired = true)_
    }

    def manageServableWithAdditionalState(servable: ServableData[Loader], aspired: Boolean
                                         )(callback: ServableState => Unit): Unit = {
      managedLock.lock()
      try {
        val harnessOpt = getHarnessInternal(servable.id)
        if (harnessOpt == null) {
          val harness = LoaderHarness(servable.id, servable.data, maxNumLoadRetries, loadRetryIntervalMicros)
          harness.setAspired(aspired)
          managedMap.addBinding(servable.id.name, harness)
          callback(new ServableState(servable.id, ManagerState.kStart))
        } else {
          LOG.info(s"This servable is already being managed: ${servable.id.toString}")
        }
      } finally {
        managedLock.unlock()
      }

    }

    def stopManagingServable(id: ServableId): Unit = {
      managedLock.lock()
      try {
        val harnessOpt = getHarnessInternal(id)
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

    private def getHarnessInternal(id: ServableId): Option[LoaderHarness] = {
      val harnessLoader = managedMap.getOrElse(id.name, null)
      if (harnessLoader != null){
        harnessLoader.find { harness => harness.id == id }
      }else{
        null
      }
    }

    def getHarnessOption(id: ServableId): Option[LoaderHarness] = {
      managedLock.lock()
      try {
        getHarnessInternal(id)
      } finally {
        managedLock.unlock()
      }
    }

  }

  class ServingMap {
    private val lock = new ReentrantReadWriteLock()
    val readLock: ReentrantReadWriteLock.ReadLock = lock.readLock()
    private val writeLock: ReentrantReadWriteLock.WriteLock = lock.writeLock()
    private val handlesMap = new mutable.HashMap[ServableRequest, LoaderHarness]()

    def availableServableIds: List[ServableId] = {
      readLock.lock()
      try {
        handlesMap.values.map { harness => harness.id }.toList
      } finally {
        readLock.unlock()
      }
    }

    def untypedServableHandle(request: ServableRequest): UntypedServableHandle = {
      readLock.lock()
      try {
        val harnessOpt = handlesMap.get(request)
        val harness = if (harnessOpt.nonEmpty) {
          harnessOpt.get
        } else {
          var selHarness: LoaderHarness = null
          val name = request.name
          request.autoVersionPolicy match {
            case AutoVersionPolicy.kLatest => {
              var version: Long = Long.MinValue
              handlesMap.values.foreach { harness =>
                if (harness.id.name == name && harness.id.version > version) {
                  selHarness = harness
                  version = harness.id.version
                }
              }
            }
            case AutoVersionPolicy.kEarliest => {
              var version: Long = Long.MaxValue
              handlesMap.values.foreach { harness =>
                if (harness.id.name == name && harness.id.version < version) {
                  selHarness = harness
                  version = harness.id.version
                }
              }
            }
          }

          selHarness
        }

        if (harness != null) {
          UntypedServableHandle(harness.id, harness.loader)
        } else {
          null.asInstanceOf[UntypedServableHandle]
        }

      } finally {
        readLock.unlock()
      }
    }

    def availableUntypedServableHandles: Map[ServableId, UntypedServableHandle] = {
      readLock.lock()
      try {
        handlesMap.map { case (_, harness) =>
          harness.id -> UntypedServableHandle(harness.id, harness.loader)
        }.toMap
      } finally {
        readLock.unlock()
      }
    }

    def update(managedMap: ManagedMap): Unit = {
      writeLock.lock()
      try {
        val managedLock = managedMap.managedLock
        managedLock.lock()
        try {
          handlesMap.clear()
          managedMap.getManagedServableNames.foreach { name =>
            managedMap.getManagedLoaderHarness(name).foreach { harness =>
              if (harness.state == LoaderHarness.State.kReady) {
                val req = new ServableRequest(harness.id.name, Some(harness.id.version))
                handlesMap.put(req, harness)
              }
            }
          }
        } finally {
          managedLock.unlock()
        }
      } finally {
        writeLock.unlock()
      }
    }
  }

}
