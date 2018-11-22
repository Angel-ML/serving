package com.tencent.angel.serving.core

import java.util.concurrent.locks.ReentrantLock

import com.tencent.angel.serving.core.EventBus.EventAndTime
import com.tencent.angel.serving.core.LoaderHarness.State.{State, _}
import com.tencent.angel.serving.core.ManagerState.ManagerState
import com.tencent.angel.serving.core.LoadOrUnloadRequest.Kind.Kind
import com.tencent.angel.serving.core.ServableRequest.AutoVersionPolicy.{AutoVersionPolicy, kLatest}
import org.slf4j.{Logger, LoggerFactory}



case class ServableId(name: String, version: Long) {
  override def toString: String = s"{name: $name, version: $version}"

  override def equals(obj: Any): Boolean = {
    val id = obj.asInstanceOf[ServableId]
    this.name == id.name && this.version == id.version
  }

  override def hashCode(): Int = {
    name.hashCode + version.hashCode()
  }

  def ==(other: ServableId): Boolean = {
    this.name == other.name && this.version == other.version
  }

  def !=(other: ServableId): Boolean = !(this == other)
}


case class ServableData[T](id: ServableId, data: T)


object ManagerState extends Enumeration {
  type ManagerState = Value
  val kStart, kLoading, kAvailable, kUnloading, kEnd = Value
}


class ServableState(val id: ServableId, var managerState: ManagerState) {
  def managerStateString(state: ManagerState): String = state match {
    case ManagerState.kStart => "Start"
    case ManagerState.kLoading => "Loading"
    case ManagerState.kAvailable => "Available"
    case ManagerState.kUnloading => "Unloading"
    case ManagerState.kEnd => "End"
  }

  override def toString: String = {
    s"id: ${id.toString}, manager_state: ${managerStateString(managerState)}"
  }

  def ==(other: ServableState): Boolean = {
    this.id == other.id && this.managerState == other.managerState
  }

  def !=(other: ServableState): Boolean = !(this == other)
}


object ServableState {
  def apply(id: ServableId, managerState: ManagerState): ServableState = {
    new ServableState(id, managerState)
  }

  implicit def toEventAndTime(state: ServableState): EventAndTime[ServableState] = {
    EventAndTime(state, System.currentTimeMillis())
  }
}


class LoaderHarness(val id: ServableId, val loader: Loader, maxNumLoadRetries: Int, loadRetryIntervalMicros: Long) {

  import LoaderHarness.ErrorCallback

  val LOG: Logger = LoggerFactory.getLogger(classOf[LoaderHarness])

  var state: State = kNew

  private val statusLock = new ReentrantLock()
  private var additionalState: Boolean = true
  private var retryFlag: Boolean = false
  var errorCallback: ErrorCallback = (id: ServableId, state: State) => {}

  private val retry = new Retry(maxNumLoadRetries, loadRetryIntervalMicros)

  def loadRequested(): Unit = transitionState(kNew, kLoadRequested)

  def loadApproved(): Unit = transitionState(kLoadRequested, kLoadApproved)

  def load(): Unit = synchronized(state) {
    assert(state == kLoadApproved)
    state = kLoading

    val retriedFn = () => {
      try {
        loader.load()
        true
      } catch {
        case LoadExceptions(msg) =>
          LOG.info(msg)
          false
        case _: Exception => false
      }
    }

    val isCancelledFn = () => {
      retryFlag
    }
    if (retry(retriedFn, isCancelledFn)) {
      state = kReady
    } else {
      state = kError
    }
  }

  def unloadRequested(): Unit = transitionState(kReady, kUnloadRequested)

  def startQuiescing(): Unit = transitionState(kUnloadRequested, kQuiescing)

  def doneQuiescing(): Unit = transitionState(kQuiescing, kQuiesced)

  def unload(): Unit = synchronized(state) {
    assert(state == kQuiesced)
    state = kUnloading
    loader.unload()
    state = kDisabled
  }

  def loaderStateSnapshot(): ServableStateSnapshot = {
    ServableStateSnapshot(id, state, additionalState)
  }

  def transitionState(from: State, to: State): Unit = synchronized(state) {
    if (state != from) {
      throw MonitorExceptions("from state does not match current state!")
    }

    state = to
  }

  def error(): Unit = synchronized(state) {
    statusLock.lock()
    try {
      if (errorCallback != null) {
        errorCallback(id, state)
      }
      state = kError
    } finally {
      statusLock.unlock()
    }
  }

  def isAspired: Boolean = additionalState

  def setAspired(aspired: Boolean): Unit = {
    additionalState = aspired
  }

  def cancelLoadRetry(): Unit = {
    retryFlag = true
  }

  def isRetryCanceled: Boolean = retryFlag

  override def equals(obj: Any): Boolean = {
    val other = obj.asInstanceOf[LoaderHarness]
    this.id == other.id && this.loader == other.loader
  }

  override def hashCode(): Int = {
    id.hashCode() + loader.hashCode()
  }

}


object LoaderHarness {

  def apply(id: ServableId, loader: Loader, maxNumLoadRetries: Int, loadRetryIntervalMicros: Long): LoaderHarness = {
    new LoaderHarness(id, loader, maxNumLoadRetries, loadRetryIntervalMicros)
  }

  object State extends Enumeration {
    type State = Value
    val kNew, kLoadRequested, kLoadApproved, kLoading, kReady = Value
    val kUnloadRequested, kQuiescing, kQuiesced, kUnloading, kDisabled, kError = Value
  }

  type ErrorCallback = (ServableId, State) => Unit
}


case class ServableStateSnapshot(id: ServableId, state: State, aspired: Boolean) {
  def ==(other: ServableStateSnapshot): Boolean = {
    this.id == other.id && this.state == other.state && this.aspired == other.aspired
  }

  def !=(other: ServableStateSnapshot): Boolean = !(this == other)
}


class ServableRequest(val name: String, val version: Option[Long] = None, val autoVersionPolicy: AutoVersionPolicy = kLatest) {

  override def hashCode(): Int = {
    val versionHash = if (version.nonEmpty) {
      version.get.hashCode()
    } else {
      autoVersionPolicy match {
        case kLatest => Long.MaxValue.hashCode()
        case kEarliest => Long.MinValue.hashCode()
      }
    }

    name.hashCode + versionHash
  }

  override def equals(obj: Any): Boolean = {
    val other = obj.asInstanceOf[ServableRequest]
    val version = if (other.version.nonEmpty && this.version.nonEmpty) {
      other.version.get == this.version.get
    } else if (other.version.isEmpty && this.version.isEmpty) {
      other.autoVersionPolicy == this.autoVersionPolicy
    } else {
      false
    }

    other.name == this.name && version
  }
}


object ServableRequest {

  object AutoVersionPolicy extends Enumeration {
    type AutoVersionPolicy = Value
    val kEarliest, kLatest = Value
  }

  def specific(name: String, version: Long): ServableRequest = new ServableRequest(name, Some(version))

  def earliest(name: String): ServableRequest = {
    val req = new ServableRequest(name, autoVersionPolicy=AutoVersionPolicy.kEarliest)
    req
  }

  def latest(name: String, version: Long): ServableRequest = {
    new ServableRequest(name, autoVersionPolicy=AutoVersionPolicy.kLatest)
  }

  def fromId(sId: ServableId): ServableRequest = {
    new ServableRequest(sId.name, Some(sId.version))
  }

}


class LoadOrUnloadRequest(val servableId: ServableId, val kind: Kind)


object LoadOrUnloadRequest {

  object Kind extends Enumeration {
    type Kind = Value
    val kLoad, kUnload = Value
  }

  def apply(servableId: ServableId, kind: Kind): LoadOrUnloadRequest = new LoadOrUnloadRequest(servableId, kind)
}


case class UntypedServableHandle(id: ServableId, loader: Loader) {
  def servable: Any = loader.servable()
}


case class ServableHandle[T](untypedHandle: UntypedServableHandle) {
  def id: ServableId = untypedHandle.id

  def servable: T = untypedHandle.servable.asInstanceOf[T]
}
