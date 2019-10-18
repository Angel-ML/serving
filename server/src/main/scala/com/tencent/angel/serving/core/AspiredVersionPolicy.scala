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

import com.tencent.angel.serving.core.AspiredVersionPolicy.{Action, ServableAction}


abstract class AspiredVersionPolicy {

  import com.tencent.angel.serving.core.AspiredVersionPolicy.ServableAction

  def getNextAction(versions: List[ServableStateSnapshot]): Option[ServableAction]

  def getHighestAspiredNewServableId(versions: List[ServableStateSnapshot]): Option[ServableId] = {
    var highestVersionId: Option[ServableId] = null
    versions.foreach { snapshot =>
      if (snapshot.aspired && snapshot.state == LoaderHarness.State.kNew) {
        if (highestVersionId == null) {
          highestVersionId = Some(snapshot.id)
        } else {
          if (highestVersionId.get.version < snapshot.id.version) {
            highestVersionId = Some(snapshot.id)
          }
        }
      }
    }

    highestVersionId
  }

  protected def getHighestServableId(versions: List[ServableStateSnapshot]): Option[ServableId] = {
    var highestVersionId: Option[ServableId] = None
    versions.foreach { snapshot =>
      if (snapshot.aspired) {
        if (highestVersionId.isEmpty) {
          highestVersionId = Some(snapshot.id)
        } else {
          if (highestVersionId.get.version < snapshot.id.version) {
            highestVersionId = Some(snapshot.id)
          }
        }
      }
    }

    highestVersionId
  }

  protected def getLowestServableId(versions: List[ServableStateSnapshot]): Option[ServableId] = {
    var lowestVersionId: Option[ServableId] = None
    versions.foreach { snapshot =>
      if (!snapshot.aspired) {
        if (lowestVersionId.isEmpty) {
          lowestVersionId = Some(snapshot.id)
        } else {
          if (lowestVersionId.get.version > snapshot.id.version) {
            lowestVersionId = Some(snapshot.id)
          }
        }
      }
    }

    lowestVersionId
  }
}


class AvailabilityPreservingPolicy extends AspiredVersionPolicy {
  override def getNextAction(versions: List[ServableStateSnapshot]): Option[AspiredVersionPolicy.ServableAction] = {
    // unload
    val unaspiredServingVersions = versions.filter { snapshot =>
      !snapshot.aspired && snapshot.state == LoaderHarness.State.kReady
    }
    if (unaspiredServingVersions.nonEmpty) {
      val idOpt = getLowestServableId(unaspiredServingVersions)
      if (idOpt.nonEmpty) {
        return Some(ServableAction(idOpt.get, Action.kUnload))
      }
    }

    // load
    val aspiredServingVersions = versions.filter { snapshot =>
      snapshot.aspired && snapshot.state == LoaderHarness.State.kNew
    }
    val idOpt = getHighestServableId(aspiredServingVersions)
    if (idOpt.nonEmpty) {
      return Some(ServableAction(idOpt.get, Action.kLoad))
    }

    None
  }
}


class ResourcePreservingPolicy extends AspiredVersionPolicy {
  override def getNextAction(versions: List[ServableStateSnapshot]): Option[AspiredVersionPolicy.ServableAction] = {
    // if there is version to be unload, unload it
    versions.foreach { snapshot =>
      if (!snapshot.aspired && snapshot.state == LoaderHarness.State.kReady) {
        return Some(ServableAction(snapshot.id, Action.kUnload))
      }
    }

    // if there is version in unload process, wait
    versions.foreach { snapshot =>
      if (!snapshot.aspired) {
        snapshot.state match {
          case LoaderHarness.State.kDisabled | LoaderHarness.State.kError => return None
          case LoaderHarness.State.kQuiesced | LoaderHarness.State.kQuiescing => return None
          case LoaderHarness.State.kUnloading | LoaderHarness.State.kUnloadRequested => return None
          case _ =>
        }
      }
    }

    // there is no version for unload or in unload process, load version
    val aspiredServingVersions = versions.filter { snapshot =>
      snapshot.aspired && snapshot.state == LoaderHarness.State.kNew
    }
    val idOpt = getHighestServableId(aspiredServingVersions)
    if (idOpt.nonEmpty) {
      return Some(ServableAction(idOpt.get, Action.kLoad))
    }

    None
  }
}


object AspiredVersionPolicy {

  def apply(classNmae: String): AspiredVersionPolicy = {
    val cls = classNmae match {
      case "AvailabilityPreservingPolicy" =>
        Class.forName(s"com.tencent.angel.serving.core.$classNmae")
      case "ResourcePreservingPolicy" =>
        Class.forName(s"com.tencent.angel.serving.core.$classNmae")
    }

    cls.newInstance().asInstanceOf[AspiredVersionPolicy]
  }

  object Action extends Enumeration {
    type Action = Value
    val kLoad, kUnload = Value
  }

  import Action.Action

  case class ServableAction(id: ServableId, action: Action)

}
