package com.tencent.angel.serving.core

import java.util
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}

import com.tencent.angel.serving.core.EventBus.EventAndTime

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class ServableStateMonitor(bus: EventBus[ServableState], maxLogEvents: Int) {
  import ManagerState._
  import ServableStateMonitor._

  private val statesLock = new ReentrantReadWriteLock()
  private val statesReadLock: ReentrantReadWriteLock.ReadLock = statesLock.readLock()
  private val statesWriteLock: ReentrantReadWriteLock.WriteLock = statesLock.writeLock()
  private val states: ServableMap = new mutable.HashMap[ServableName, VersionMap]()

  private val liveStatesLock = new ReentrantReadWriteLock()
  private val liveStatesReadLock: ReentrantReadWriteLock.ReadLock = liveStatesLock.readLock()
  private val liveStatesWriteLock: ReentrantReadWriteLock.WriteLock = liveStatesLock.writeLock()
  private val liveStates: ServableMap = new mutable.HashMap[ServableName, VersionMap]()

  private val logLock = new ReentrantReadWriteLock()
  private val logReadLock: ReentrantReadWriteLock.ReadLock = logLock.readLock()
  private val logWriteLock: ReentrantReadWriteLock.WriteLock = logLock.writeLock()
  private val log: BoundedLog = new util.ArrayDeque[ServableStateAndTime]((maxLogEvents*1.2).toInt)

  bus.subscribe(handleEvent)

  private def handleEvent(stateAndTime: ServableStateAndTime): Unit = {
    val name = stateAndTime.event.id.name
    val version = stateAndTime.event.id.version

    statesWriteLock.lock()
    try {
      if (states.contains(name)) {
        states(name)(version) = stateAndTime
      } else {
        val versionMap = new mutable.HashMap[Version, ServableStateAndTime]()
        versionMap(version) = stateAndTime
        states(name) = versionMap
      }
    } finally {
      statesWriteLock.unlock()
    }

    updateLiveStates(stateAndTime)

    maybeSendStateReachedNotifications()

    logWriteLock.lock()
    try {
      while (log.size() >= maxLogEvents) {
        log.poll()
      }

      log.push(stateAndTime)
    } finally {
      logWriteLock.unlock()
    }

  }

  private def updateLiveStates(stateAndTime: ServableStateAndTime): Unit = {
    val name = stateAndTime.event.id.name
    val version = stateAndTime.event.id.version

    liveStatesWriteLock.lock()
    try {
      if (stateAndTime.event.managerState != ManagerState.kEnd) {
        if (liveStates.contains(name)) {
          liveStates(name)(version) = stateAndTime
        } else {
          val versionMap = new mutable.HashMap[Version, ServableStateAndTime]()
          versionMap(version) = stateAndTime
          liveStates(name) = versionMap
        }
      } else {
        if (liveStates.contains(name) && liveStates(name).contains(version)) {
          liveStates(name).remove(version)
        }
      }
    } finally {
      liveStatesWriteLock.unlock()
    }
  }

  def getStateAndTime(servableId: ServableId): Option[ServableStateAndTime] = {
    val name = servableId.name
    val version = servableId.version

    statesReadLock.lock()
    try {
      if (states.contains(name) && states(name).contains(version)) {
        Some(states(name)(version))
      } else {
        None
      }
    } finally {
      statesReadLock.unlock()
    }
  }

  def getState(servableId: ServableId): Option[ServableState] = {
    val servableStateAndTime = getStateAndTime(servableId)
    if (servableStateAndTime.isEmpty) {
      None
    } else {
      Some(servableStateAndTime.get.event)
    }
  }

  def getVersionStates(servableName: String): VersionMap = {
    statesReadLock.lock()
    try{
      states.getOrElse(servableName, new mutable.HashMap[Version, ServableStateAndTime]())
    } finally {
      statesReadLock
    }
  }

  def getAllServableStates: ServableMap = {
    statesReadLock.lock()
    try{
      states
    } finally {
      statesReadLock.unlock()
    }
  }

  def getLiveServableStates: ServableMap = {
    liveStatesReadLock.lock()
    try{
      liveStates
    } finally {
      liveStatesReadLock.unlock()
    }
  }

  def getBoundedLog: BoundedLog = {
    logReadLock.lock()
    try{
      log
    } finally {
      logReadLock.unlock()
    }
  }

  //--------------------------------------------------------------------

  val servableStateNotificationRequests = new ListBuffer[ServableStateNotificationRequest]()

  def waitUntilServablesReachState(servables: List[ServableRequest], goalState: ManagerState): Unit = {
    val lock = new ReentrantLock()
    val cond = lock.newCondition()

    notifyWhenServablesReachState(servables, goalState,
      (idStateMap: Map[ServableId, ManagerState]) => { cond.signal() }
    )

    cond.await()
  }

  def notifyWhenServablesReachState(servables: List[ServableRequest], goalState: ManagerState,
                                    func: ServableStateNotifierFn): Unit = {
    servableStateNotificationRequests.append(ServableStateNotificationRequest(servables, goalState, func))
    maybeSendStateReachedNotifications()
  }

  private def maybeSendStateReachedNotifications(): Unit = {
    servableStateNotificationRequests.foreach{ case (servables, goalState, func) =>

    }
  }

  private def shouldSendStateReachedNotification()

}

object ServableStateMonitor {
  import ManagerState._

  type ServableName = String

  type Version = Long

  type ServableStateAndTime = EventAndTime[ServableState]

  type VersionMap = mutable.HashMap[Version, ServableStateAndTime]

  type ServableMap = mutable.HashMap[ServableName, VersionMap]

  type BoundedLog = util.Deque[ServableStateAndTime]

  type ServableStateNotifierFn = Map[ServableId, ManagerState] => Unit

  case class ServableStateNotificationRequest(servables: List[ServableRequest], goalState: ManagerState,
                                              notifierFn: ServableStateNotifierFn)
}
