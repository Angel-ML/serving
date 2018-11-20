package com.tencent.angel.serving.core

import com.tencent.angel.serving.core.LoaderHarness.State._


case class ServableId(name: String, version: Long) {
  override def toString: String = s"{name: $name, version: $version}"

  def ==(other: ServableId): Boolean = {
    this.name == other.name && this.version == other.version
  }

  def !=(other: ServableId): Boolean = !(this == other)
}


case class ServableData[T](id: ServableId, status_ : Status, data: T){
  def status: Status = status_
}


case class Aspired(private var aspired: Boolean) {
  def setAspired(aspiredValue: Boolean): Unit = {
    aspired = aspiredValue
  }

  def isAspired: Boolean = aspired
}


case class ServableStateSnapshot[T](id: ServableId, state: State, additionalState: Option[T]) {
  def ==(other: ServableStateSnapshot[T]): Boolean = {
    this.id == other.id && this.state == other.state && this.additionalState == other.additionalState
  }

  def !=(other: ServableStateSnapshot[T]): Boolean = !(this == other)
}


class ServableRequest(val name: String, val version: Option[Long] = None) {

  import ServableRequest.AutoVersionPolicy._

  var autoVersionPolicy: AutoVersionPolicy = kLatest
}


object ServableRequest {

  object AutoVersionPolicy extends Enumeration {
    type AutoVersionPolicy = Value
    val kEarliest, kLatest = Value
  }

  def specific(name: String, version: Long): ServableRequest = new ServableRequest(name, Some(version))

  def earliest(name: String): ServableRequest = {
    val req = new ServableRequest(name)
    req.autoVersionPolicy = AutoVersionPolicy.kEarliest
    req
  }

  def latest(name: String, version: Long): ServableRequest = new ServableRequest(name)

  def fromId(sId: ServableId): ServableRequest = {
    new ServableRequest(sId.name, Some(sId.version))
  }

}


case class UntypedServableHandle(id: ServableId, loader: Loader) {
  def servable: Any = loader.servable()
}


case class ServableHandle[T](untypedHandle: UntypedServableHandle) {
  def id: ServableId = untypedHandle.id

  def servable: T = untypedHandle.servable.asInstanceOf[T]
}
