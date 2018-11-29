package com.tencent.angel.serving.core

import java.util
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}

import com.tencent.angel.serving.core.EventBus.EventAndTime
import com.tencent.angel.serving.core.ServableRequest.AutoVersionPolicy
import scala.collection.mutable


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
  private val log: BoundedLog = new util.ArrayDeque[ServableStateAndTime]((maxLogEvents * 1.2).toInt)

  bus.subscribe(handleEvent)

  private def handleEvent(stateAndTime: ServableStateAndTime): Unit = {
    val name = stateAndTime.state.id.name
    val version = stateAndTime.state.id.version

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
    val name = stateAndTime.state.id.name
    val version = stateAndTime.state.id.version

    liveStatesWriteLock.lock()
    try {
      if (stateAndTime.state.managerState != ManagerState.kEnd) {
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
      Some(servableStateAndTime.get.state)
    }
  }

  def getVersionStates(servableName: String): VersionMap = {
    statesReadLock.lock()
    try {
      states.getOrElse(servableName, new mutable.HashMap[Version, ServableStateAndTime]())
    } finally {
      statesReadLock
    }
  }

  def getAllServableStates: ServableMap = {
    statesReadLock.lock()
    try {
      states
    } finally {
      statesReadLock.unlock()
    }
  }

  def getLiveServableStates: ServableMap = {
    liveStatesReadLock.lock()
    try {
      liveStates
    } finally {
      liveStatesReadLock.unlock()
    }
  }

  def getBoundedLog: BoundedLog = {
    logReadLock.lock()
    try {
      log
    } finally {
      logReadLock.unlock()
    }
  }

  //--------------------------------------------------------------------

  private val notificationLock = new ReentrantLock()
  private val servableStateNotificationRequests = new util.ArrayList[ServableStateNotificationRequest]()

  def waitUntilServablesReachState(servables: List[ServableRequest], goalState: ManagerState): Map[ServableId, ManagerState] = {
    val lock = new ReentrantLock()
    val cond = lock.newCondition()
    var condFlag = false
    var reachedState: Map[ServableId, ManagerState] = null

    // return when one of the ServableRequest reach the goalState, not all
    notifyWhenServablesReachState(servables, goalState,
      (idStateMap: Map[ServableId, ManagerState]) => {
        if (idStateMap != null && idStateMap.nonEmpty) {
          condFlag = true
          cond.signal()
          reachedState = idStateMap
        }
      }
    )

    while (!condFlag) {
      cond.await()
    }
    reachedState
  }

  def notifyWhenServablesReachState(servables: List[ServableRequest], goalState: ManagerState,
                                    func: ServableStateNotifierFn): Unit = {
    notificationLock.lock()
    try {
      servableStateNotificationRequests.add(ServableStateNotificationRequest(servables, goalState, func))
    } finally {
      notificationLock.unlock()
    }

    maybeSendStateReachedNotifications()

  }

  private def maybeSendStateReachedNotifications(): Unit = {
    notificationLock.lock()
    try {
      val iter = servableStateNotificationRequests.iterator()

      while (iter.hasNext) {
        val notificationRequest = iter.next()
        val optStateAndStatesReached = shouldSendStateReachedNotification(notificationRequest)
        if (optStateAndStatesReached != null && optStateAndStatesReached.nonEmpty) {
          notificationRequest.notifierFn(optStateAndStatesReached.get)
          iter.remove()
        }
      }
    } finally {
      notificationLock.unlock()
    }

  }

  private def shouldSendStateReachedNotification(notificationRequest: ServableStateNotificationRequest
                                                ): Option[Map[ServableId, ManagerState]] = {
    statesReadLock.lock()
    val ret = new mutable.HashMap[ServableId, ManagerState]()
    val goalState = notificationRequest.goalState

    try {
      notificationRequest.servables.foreach { servableRequest =>
        val name = servableRequest.name
        if (states.contains(name)) {
          val versionMap = states(name)

          if (versionMap != null && versionMap.nonEmpty) {
            val version = if (servableRequest.version.nonEmpty) {
              servableRequest.version.get
            } else {
              servableRequest.autoVersionPolicy match {
                case AutoVersionPolicy.kEarliest => versionMap.keys.min
                case AutoVersionPolicy.kLatest => versionMap.keys.max
              }
            }

            if (versionMap.contains(version)) {
              val servableId = ServableId(name, version)
              val managerState = versionMap(version).state.managerState
              if (goalState == managerState) {
                ret(servableId) = managerState
              }
            }
          }
        }
      }
    } finally {
      statesReadLock.unlock()
    }

    if (ret.isEmpty) {
      None
    } else {
      Some(ret.toMap)
    }
  }

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
