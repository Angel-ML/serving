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

object LoadServablesFast {
  def getManagerNumLoadThreads(manager: AspiredVersionsManager): Int = {
    manager.numLoadThreads
  }

  def setManagerNumLoadThreadsNotifier(manager: AspiredVersionsManager): Int => Unit = {
    ???
  }

  def connectSourcesWithFastInitialLoad(aspiredVersionsManager: AspiredVersionsManager, sources: List[Source[Loader]],
                                        waitUntilLoaded: () => Unit, numThreads: Int): Unit = {
    // val preNumLoadThreads = getManagerNumLoadThreads(aspiredVersionsManager)
    // val setManagerNumLoadThreads = setManagerNumLoadThreadsNotifier(aspiredVersionsManager)
    // setManagerNumLoadThreads(numThreads)
    sources.foreach(source => ConnectSourceToTarget(source, aspiredVersionsManager))
    waitUntilLoaded()
    // setManagerNumLoadThreads(preNumLoadThreads)
  }


  def connectSourcesWithFastInitialLoad(aspiredVersionsManager: AspiredVersionsManager, sources: List[Source[Loader]],
                                        servableStateMonitor: ServableStateMonitor, initialServables: List[ServableRequest],
                                        numThreads: Int): Unit = {
    // type WaitUntilLoaded = Unit => Unit
    val waitUntilLoaded = () => {
      val statesReached = servableStateMonitor.waitUntilServablesReachState(initialServables, ManagerState.kAvailable)
      if (statesReached.isEmpty) {
        val numUnavailableModels = statesReached.count(stateReached => stateReached._2 != ManagerState.kAvailable)
        val message = String.join(numUnavailableModels.toString, "servable(s) did not become avaible:")
        statesReached.collect {
          case (servableId, managerState) if managerState != ManagerState.kAvailable =>
            message.concat(s"{${servableId.toString}}")
        }
        throw new Exception(message)
      }
    }
    connectSourcesWithFastInitialLoad(aspiredVersionsManager, sources, waitUntilLoaded, numThreads)
  }

  def connectSourceWithFastInitialLoad(aspiredVersionsManager: AspiredVersionsManager, source: Source[Loader],
                                       servableStateMonitor: ServableStateMonitor, initialServables: List[ServableRequest],
                                       numThreads: Int): Unit = {
    connectSourcesWithFastInitialLoad(aspiredVersionsManager, List(source), servableStateMonitor, initialServables, numThreads)
  }
}
