package com.tencent.angel.serving.core


object ManagerState extends Enumeration {
  type ManagerState = Value
  val kStart, kLoading, kAvailable, kUnloading, kEnd = Value
}

import ManagerState._

class ServableState(val id: ServableId, var managerState: ManagerState, var health: Status) {
  def managerStateString(state: ManagerState): String = state match {
    case ManagerState.kStart => "Start"
    case ManagerState.kLoading => "Loading"
    case ManagerState.kAvailable => "Available"
    case ManagerState.kUnloading => "Unloading"
    case ManagerState.kEnd => "End"
  }

  override def toString: String = {
    s"id: ${id.toString}, manager_state: ${managerStateString(managerState)}, health: ${health.toString}"
  }

  def ==(other:ServableState): Boolean = {
    this.id == other.id && this.managerState == other.managerState && this.health == other.health
  }

  def !=(other:ServableState): Boolean = !(this == other)
}

object ServableState {
  def apply(id: ServableId, managerState: ManagerState, health: Status): ServableState = {
    new ServableState(id, managerState, health)
  }
}

