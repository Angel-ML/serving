package com.tencent.angel.serving.core

import java.util
import java.util.{Timer, TimerTask}
import java.util.concurrent.locks.ReentrantLock

import com.tencent.angel.serving.core.AspiredVersionPolicy.{Action, ServableAction}


class AspiredVersionsManager private(
                                      val manageStateIntervalMicros: Long,
                                      val aspiredVersionPolicy: AspiredVersionPolicy,
                                      val basicManager: BasicManager
                                    ) extends Target[Loader] with Manager {

  import com.tencent.angel.serving.core.LoaderHarness.State._
  import com.tencent.angel.serving.core.AspiredVersionsManager._

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

  private def flushServables(): Unit = {
    // remove element form basicManager
    basicManager.getManagedServableNames.foreach { servableName =>
      val stateSnapshots = basicManager.getManagedServableStateSnapshots(servableName)
      stateSnapshots.foreach { stateSnapshot =>
        if (stateSnapshot.additionalState.nonEmpty) {
          val state = stateSnapshot.state
          val isAspired = stateSnapshot.additionalState.get.isAspired
          if (state == kNew || state == kDisabled || state == kError || !isAspired) {
            basicManager.stopManagingServable(stateSnapshot.id)
          }
        }
      }
    }
  }

  private def handlePendingAspiredVersionsRequests(): Unit = {
    // handle requests and remove them from `pendingAspiredVersionsRequests`
    versionsRequestsLock.lock()
    try {
      val iter = pendingAspiredVersionsRequests.keySet().iterator()
      while (iter.hasNext) {
        val name = iter.next()
        val versions = pendingAspiredVersionsRequests.get(name)

        if (processAspiredVersionsRequest(name, versions)) {
          iter.remove() // also remove from pendingAspiredVersionsRequests
        }
      }
    } finally {
      versionsRequestsLock.unlock()
    }
  }

  private def invokePolicyAndExecuteAction(): Unit = {
    val nextAction = getNextAction
    if (nextAction.nonEmpty) {
      performAction(nextAction.get)
    }
  }

  private def processAspiredVersionsRequest(servableName: String, versions: List[ServableData[Loader]]): Boolean = {
    val newAspiredVersions = versions.map(version => version.id.version).toSet

    val servableStateSnapshots = basicManager.getManagedServableStateSnapshots(servableName)
    val oldInvalidateAspiredVersions = servableStateSnapshots.collect {
      case snapshot if snapshot.additionalState.nonEmpty && !snapshot.additionalState.get.isAspired =>
        snapshot.id.version
    }.toSet


    if ((oldInvalidateAspiredVersions -- newAspiredVersions).nonEmpty) {
      false // Sit on it for now. We'll check again later
    } else {
      val oldWorkingAspiredVersions = servableStateSnapshots.collect {
        case snapshot if snapshot.additionalState.nonEmpty && snapshot.additionalState.get.isAspired =>
          snapshot.id.version
      }.toSet

      val toLoad = newAspiredVersions -- oldWorkingAspiredVersions
      val toUnload = oldWorkingAspiredVersions -- newAspiredVersions

      // first deal with unload
      versions.foreach {
        case version if toUnload.contains(version.id.version) => // unload
          basicManager.getAdditionalServableState(version.id).setAspired(false)
          basicManager.cancelLoadServableRetry(version.id)
        case _ => // nothing to do
      }

      // then deal with load
      versions.foreach {
        case version if toLoad.contains(version.id.version) => // load
          basicManager.manageServableWithAdditionalState(version, aspired = true)
        case _ => // nothing to do
      }

      true
    }
  }

  private def getNextAction: Option[ServableAction] = {
    val actions = basicManager.getManagedServableNames.map { name =>
      val snapshots = basicManager.getManagedServableStateSnapshots(name)
      aspiredVersionPolicy.getNextAction(snapshots)
    }

    val kUnloads = actions.collect { case action if action.nonEmpty && action.get.action == Action.kUnload => action }

    if (kUnloads.isEmpty) {
      val kLoads = actions.collect { case action if action.nonEmpty && action.get.action == Action.kLoad => action }
      if (kLoads.isEmpty) {
        None
      } else {
        kLoads.head
      }
    } else {
      kUnloads.head
    }
  }

  private def performAction(servableAction: ServableAction): Unit = {
    servableAction match {
      case Action.kLoad => basicManager.loadServable(servableAction.id)
      case Action.kUnload => basicManager.unloadServable(servableAction.id)
    }
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
