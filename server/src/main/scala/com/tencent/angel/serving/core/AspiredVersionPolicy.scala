package com.tencent.angel.serving.core


abstract class AspiredVersionPolicy {
  import com.tencent.angel.serving.core.AspiredVersionPolicy.ServableAction

  def getNextAction(versions: List[ServableStateSnapshot[Aspired]]): Option[ServableAction]

  def getHighestAspiredNewServableId(versions: List[ServableStateSnapshot[Aspired]]): Option[ServableId] = {
    var highestVersionId: Option[ServableId] = null
    versions.foreach { snapshot =>
      if (snapshot.additionalState.isDefined && snapshot.additionalState.get.isAspired &&
        snapshot.state == LoaderHarness.State.kNew) {
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

  protected def getHighestServableId(versions: List[ServableStateSnapshot[Aspired]]): Option[ServableId] = {
    var highestVersionId: Option[ServableId] = null
    versions.foreach { snapshot =>
      if (snapshot.additionalState.isDefined && snapshot.additionalState.get.isAspired) {
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

  protected def getLowestServableId(versions: List[ServableStateSnapshot[Aspired]]): Option[ServableId] = {
    var lowestVersionId: Option[ServableId] = null
    versions.foreach { snapshot =>
      if (snapshot.additionalState.isDefined && snapshot.additionalState.get.isAspired) {
        if (lowestVersionId == null) {
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

}

class ResourcePreservingPolicy extends AspiredVersionPolicy {

}


object AspiredVersionPolicy {
  object Action extends Enumeration {
    type Action = Value
    val kLoad, kUnload = Value
  }

  import Action.Action
  case class ServableAction(id: ServableId, action: Action)
}
