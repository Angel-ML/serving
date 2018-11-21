package com.tencent.angel.serving.core

import java.util.concurrent.locks.ReentrantLock

import com.tencent.angel.serving.core.EventBus.EventAndTime
import com.tencent.angel.serving.core.LoaderHarness.State._
import com.tencent.angel.serving.core.ManagerState.ManagerState
import com.tencent.angel.serving.core.LoadOrUnloadRequest.Kind.Kind

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


case class Aspired(private var aspired: Boolean) {
  def setAspired(aspiredValue: Boolean): Unit = {
    aspired = aspiredValue
  }

  def isAspired: Boolean = aspired
}


class LoaderHarness(val id: ServableId, val loader: Loader, maxNumLoadRetries: Int, loadRetryIntervalMicros: Long) {

  import LoaderHarness.ErrorCallback

  var state: State = kNew

  private val statusLock = new ReentrantLock()
  var status: ManagerState = _
  val additionalState: Aspired = Aspired(true)
  var cancelLoadRetry: Boolean = false
  var errorCallback: ErrorCallback = _

  def loadRequested(): Unit = transitionState(kNew, kLoadRequested)

  def loadApproved(): Unit = transitionState(kLoadRequested, kLoadApproved)

  def load(): Unit = synchronized(state) {
    assert(state == kLoadApproved)
    state = kLoading
    loader.load()
    state = kReady
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

  def loaderStateSnapshot(): ServableStateSnapshot[Aspired] = {
    ServableStateSnapshot[Aspired](id, state, Some(additionalState))
  }

  def transitionState(from: State, to: State): Unit = synchronized(state) {
    if (status != from) {
      throw MonitorExceptions("from state does not match current state!")
    }

    state = to
  }

  def error(mStatus: ManagerState): Unit = synchronized(state) {
    state = kError

    statusLock.lock()
    try {
      status = mStatus
      if (errorCallback != null) {
        errorCallback(id, status)
      }
    } finally {
      statusLock.unlock()
    }
  }

  def isAspired: Boolean = additionalState.isAspired

  def setAspired(aspired: Boolean): Unit = additionalState.setAspired(aspired)

  override def equals(obj: Any): Boolean = {
    val other = obj.asInstanceOf[LoaderHarness]
    this.id == other.id && this.loader == other.loader &&
      this.loadRetryIntervalMicros == other.loadRetryIntervalMicros &&
      this.maxNumLoadRetries == other.maxNumLoadRetries
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

  type ErrorCallback = (ServableId, ManagerState) => Unit
}


case class ServableStateSnapshot[T](id: ServableId, state: State, additionalState: Option[T]) {
  def ==(other: ServableStateSnapshot[T]): Boolean = {
    this.id == other.id && this.state == other.state && this.additionalState == other.additionalState
  }

  def !=(other: ServableStateSnapshot[T]): Boolean = !(this == other)
}


class ServableRequest(val name: String, val version: Option[Long] = None) {

  import ServableRequest.AutoVersionPolicy._

  var autoVersionPolicy: AutoVersionPolicy = kLatest
}


object ServableRequest {

  object AutoVersionPolicy extends Enumeration {
    type AutoVersionPolicy = Value
    val kEarliest, kLatest = Value
  }

  def specific(name: String, version: Long): ServableRequest = new ServableRequest(name, Some(version))

  def earliest(name: String): ServableRequest = {
    val req = new ServableRequest(name)
    req.autoVersionPolicy = AutoVersionPolicy.kEarliest
    req
  }

  def latest(name: String, version: Long): ServableRequest = new ServableRequest(name)

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
