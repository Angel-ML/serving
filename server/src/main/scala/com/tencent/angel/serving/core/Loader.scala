package com.tencent.angel.serving.core

trait Loader {
  def estimateResources(): ResourceAllocation

  def load(): Unit

  def unload(): Unit

  def servable(): Any
}
