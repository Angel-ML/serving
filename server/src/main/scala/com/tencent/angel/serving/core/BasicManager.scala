package com.tencent.angel.serving.core

class BasicManager extends Manager {

  //---------------------------------------------------------------------------GetInfo
  def getServableStateSnapshots(servableName: String): List[ServableStateSnapshot[Aspired]] = {

  }

  def getManagedServableNames: List[String] = {

  }

  //---------------------------------------------------------------------------
  def stopManagingServable(id: ServableId)

  //---------------------------------------------------------------------------Manager
  override def availableServableIds: List[ServableId] = ???

  override def availableServableHandles[T]: Map[ServableId, ServableHandle[T]] = ???

  override def servableHandle[T](request: ServableRequest): ServableHandle[T] = ???

  override def untypedServableHandle(request: ServableRequest): UntypedServableHandle = ???

  override def availableUntypedServableHandles: Map[ServableId, UntypedServableHandle] = ???
}
