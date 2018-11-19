package com.tencent.angel.serving.core


trait Manager {
  def availableServableIds: List[ServableId]

  def availableServableHandles[T]: Map[ServableId, ServableHandle[T]]

  def servableHandle[T](request: ServableRequest): ServableHandle[T]

  def untypedServableHandle(request: ServableRequest): UntypedServableHandle

  def availableUntypedServableHandles: Map[ServableId, UntypedServableHandle]
}
