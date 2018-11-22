package com.tencent.angel.serving.core

import com.tencent.angel.confg.ResourceAllocation

trait Loader {
  def estimateResources(): ResourceAllocation

  def load(): Unit

  def unload(): Unit

  def servable(): Any
}
