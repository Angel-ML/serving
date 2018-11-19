package com.tencent.angel.serving.core

trait Source[T] {
  def setAspiredVersionsCallback(callback: AspiredVersionsCallback[T]): Unit
}
