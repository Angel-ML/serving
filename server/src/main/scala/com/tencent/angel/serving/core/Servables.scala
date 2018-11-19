package com.tencent.angel.serving.core


case class ServableId(name: String, version: Long) {
  override def toString: String = s"{name: $name, version: $version}"

  def ==(other: ServableId): Boolean = {
    this.name == other.name && this.version == other.version
  }

  def !=(other: ServableId): Boolean = !(this == other)
}


case class ServableData[T](id: ServableId, status: Status, data: T)


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
