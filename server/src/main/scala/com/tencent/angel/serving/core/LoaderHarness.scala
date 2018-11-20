package com.tencent.angel.serving.core

import java.util.concurrent.locks.ReentrantLock

import com.tencent.angel.serving.core.ManagerState.ManagerState


class LoaderHarness(val id: ServableId, val loader: Loader, maxNumLoadRetries: Int,
                    loadRetryIntervalMicros: Long) {
  import LoaderHarness.State._
  import LoaderHarness.ErrorCallback

  var state: State = kNew

  val statusLock = new ReentrantLock()
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
    try{
      status = mStatus
      if (errorCallback != null) {
        errorCallback(id, status)
      }
    } finally {
      statusLock.unlock()
    }
  }

  def isAspired: Boolean = additionalState.isAspired
}


object LoaderHarness {
  object State extends Enumeration {
    type State = Value
    val kNew, kLoadRequested, kLoadApproved, kLoading, kReady = Value
    val kUnloadRequested, kQuiescing, kQuiesced, kUnloading, kDisabled, kError = Value
  }

  type ErrorCallback = (ServableId, ManagerState) => Unit
}
