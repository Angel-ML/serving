package com.tencent.angel.serving.core


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
    var highestVersionId: Option[ServableId] = null
    versions.foreach { snapshot =>
      if (snapshot.aspired) {
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

  protected def getLowestServableId(versions: List[ServableStateSnapshot]): Option[ServableId] = {
    var lowestVersionId: Option[ServableId] = null
    versions.foreach { snapshot =>
      if (snapshot.aspired) {
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
  override def getNextAction(versions: List[ServableStateSnapshot]): Option[AspiredVersionPolicy.ServableAction] = ???
}


class ResourcePreservingPolicy extends AspiredVersionPolicy {
  override def getNextAction(versions: List[ServableStateSnapshot]): Option[AspiredVersionPolicy.ServableAction] = ???
}


object AspiredVersionPolicy {
  object Action extends Enumeration {
    type Action = Value
    val kLoad, kUnload = Value
  }

  import Action.Action
  case class ServableAction(id: ServableId, action: Action)
}
