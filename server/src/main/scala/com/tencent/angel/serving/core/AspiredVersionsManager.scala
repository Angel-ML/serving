/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.serving.core

import java.util
import java.util.{Timer, TimerTask}
import java.util.concurrent.locks.ReentrantLock

import com.tencent.angel.config.ResourceAllocation
import com.tencent.angel.serving.core.LoaderHarness.State._
import com.tencent.angel.serving.core.AspiredVersionsManager._
import com.tencent.angel.serving.core.AspiredVersionPolicy.{Action, ServableAction}


class AspiredVersionsManager (
                               manageStateDelayMicros: Long,
                               manageStateIntervalMicros: Long,
                               aspiredVersionPolicy: AspiredVersionPolicy,
                               var numLoadThreads: Int, numUnloadThreads: Int,
                               maxNumLoadRetries: Int, loadRetryIntervalMicros: Long,
                               totalResources: ResourceAllocation,
                               servableEventBus: EventBus[ServableState]
                             ) extends Target[Loader] with Manager {
  val basicManager: BasicManager = new BasicManager(numLoadThreads, numUnloadThreads, maxNumLoadRetries,
    loadRetryIntervalMicros, totalResources, servableEventBus)
  private val versionsRequestsLock = new ReentrantLock()
  private val pendingAspiredVersionsRequests: AspiredVersionsMap = new util.HashMap[String, List[ServableData[Loader]]]()

  private val manageStateThread = new Timer("PeriodicFunction", true)
  manageStateThread.scheduleAtFixedRate(new TimerTask {
    override def run(): Unit = {
      flushServables()
      handlePendingAspiredVersionsRequests()
      invokePolicyAndExecuteAction()
    }
  }, manageStateDelayMicros, manageStateIntervalMicros)

  private def flushServables(): Unit = {
    // remove element form basicManager
    basicManager.getManagedServableNames.foreach { servableName =>
      val stateSnapshots = basicManager.getManagedServableStateSnapshots(servableName)
      stateSnapshots.foreach { stateSnapshot =>
        val state = stateSnapshot.state
        if (state == kNew || state == kDisabled || state == kError || !stateSnapshot.aspired) {
          basicManager.stopManagingServable(stateSnapshot.id)
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
      case snapshot if !snapshot.aspired => snapshot.id.version
    }.toSet


    if ((oldInvalidateAspiredVersions -- newAspiredVersions).nonEmpty) {
      false // Sit on it for now. We'll check again later
    } else {
      val oldVersions = servableStateSnapshots.collect {
        case snapshot if snapshot.aspired =>
          snapshot
      }.toSet
      val oldWorkingAspiredVersions = oldVersions.map {
        case version => version.id.version
      }

      val toLoad = newAspiredVersions -- oldWorkingAspiredVersions
      val toUnload = oldWorkingAspiredVersions -- newAspiredVersions

      // first deal with unload
      oldVersions.foreach {
        case version if toUnload.contains(version.id.version) => // unload
          basicManager.setAspiredState(version.id, aspired = false)
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
    servableAction.action match {
      case Action.kLoad => basicManager.loadServable(servableAction.id)
      case Action.kUnload => basicManager.unloadServable(servableAction.id)
    }
  }


  //---------------------------------------------------------------------------Manager
  override def availableServableIds: List[ServableId] = {
    basicManager.availableServableIds
  }

  override def availableServableHandles[T]: Map[ServableId, ServableHandle[T]] = {
    basicManager.availableServableHandles[T]
  }

  override def servableHandle[T](request: ServableRequest): ServableHandle[T] = {
    basicManager.servableHandle[T](request)
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

  private def enqueueAspiredVersionsRequest(servableName: String, versions: List[ServableData[Loader]]): Unit = {
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
