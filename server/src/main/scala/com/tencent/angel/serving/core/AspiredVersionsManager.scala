package com.tencent.angel.serving.core

import java.util
import java.util.{Timer, TimerTask}
import java.util.concurrent.locks.ReentrantLock

import com.tencent.angel.serving.core.AspiredVersionPolicy.ServableAction



class AspiredVersionsManager private(
                                      val manageStateIntervalMicros: Long,
                                      val aspiredVersionPolicy: AspiredVersionPolicy,
                                      val basicManager: BasicManager
                                    ) extends Target[Loader] with Manager {
  import LoaderHarness.State._
  import AspiredVersionsManager._

  val versionsRequestsLock = new ReentrantLock()
  val pendingAspiredVersionsRequests: AspiredVersionsMap = new util.HashMap[String, List[ServableData[Loader]]]()

  val manageStateThread = new Timer("PeriodicFunction", true)

  manageStateThread.scheduleAtFixedRate(new TimerTask {
    override def run(): Unit = {
      flushServables()
      handlePendingAspiredVersionsRequests()
      invokePolicyAndExecuteAction()
    }
  }, 1000, manageStateIntervalMicros)

  def flushServables(): Unit = {
    basicManager.getManagedServableNames.foreach{servableName =>
      val stateSnapshots = basicManager.getServableStateSnapshots(servableName)
      stateSnapshots.foreach{ stateSnapshot =>
        if (stateSnapshot.additionalState.nonEmpty){
          val state = stateSnapshot.state
          val isAspired = stateSnapshot.additionalState.get.isAspired
          if (state == kNew || state == kDisabled || state == kError || !isAspired) {
            basicManager.stopManagingServable(stateSnapshot.id)
          }
        }
      }
    }

  }

  def handlePendingAspiredVersionsRequests(): Unit = {
    versionsRequestsLock.lock()
    try {
      val iter = pendingAspiredVersionsRequests.keySet().iterator()
      while(iter.hasNext) {
        val name = iter.next()
        val versions = pendingAspiredVersionsRequests.get(name)

        if (!containsAnyReaspiredVersions(name, versions)) {
          processAspiredVersionsRequest(name, versions)
          iter.remove() // also remove from pendingAspiredVersionsRequests
        }
      }
    } finally {
      versionsRequestsLock.unlock()
    }
  }

  def invokePolicyAndExecuteAction(): Unit = {
    val nextAction = getNextAction()
    if (nextAction.nonEmpty) {
      performAction(nextAction.get)
    }
  }

  def processAspiredVersionsRequest(servableName: String, versions: List[ServableData[Loader]]): Unit = {}

  def containsAnyReaspiredVersions(servableName: String, versions: List[ServableData[Loader]]): Boolean = {}

  def getNextAction(): Option[ServableAction] = {

  }

  def performAction(servableAction: ServableAction): Unit = {

  }


  //---------------------------------------------------------------------------Manager

  override def availableServableIds: List[ServableId] = {
    basicManager.availableServableIds
  }

  override def availableServableHandles[Loader]: Map[ServableId, ServableHandle[Loader]] = {
    basicManager.availableServableHandles[Loader]
  }

  override def servableHandle[Loader](request: ServableRequest): ServableHandle[Loader] = {
    basicManager.servableHandle[Loader](request)
  }

  override def untypedServableHandle(request: ServableRequest): UntypedServableHandle = {
    basicManager.untypedServableHandle(request)
  }

  override def availableUntypedServableHandles: Map[ServableId, UntypedServableHandle] = {
    basicManager.availableUntypedServableHandles
  }

  //---------------------------------------------------------------------------Target
  override def getAspiredVersionsCallback: AspiredVersionsCallback[Loader] = {
    (servableName: String, versions: List[ServableData[Loader]]) => {
      enqueueAspiredVersionsRequest(servableName, versions)
    }
  }

  def enqueueAspiredVersionsRequest(servableName: String, versions: List[ServableData[Loader]]): Unit = {
    val validationStatus = versions.forall(version => version.id.name == servableName)

    versionsRequestsLock.lock()
    try {
      if (validationStatus) {
        pendingAspiredVersionsRequests.put(servableName, versions)
      }
    } finally {
      versionsRequestsLock.unlock()
    }

  }
}

object AspiredVersionsManager {
  type AspiredVersionsMap = util.HashMap[String, List[ServableData[Loader]]]
}
