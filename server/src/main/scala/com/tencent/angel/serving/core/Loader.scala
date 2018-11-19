package com.tencent.angel.serving.core

trait Loader {
  def estimateResources(): ResourceAllocation

  def load(): Unit

  def unLoad(): Unit

  def servable(): Any
}
