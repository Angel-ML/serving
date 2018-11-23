package com.tencent.angel.serving.core

import com.tencent.angel.config.ResourceAllocation

trait Loader {
  def estimateResources(): ResourceAllocation

  def load(): Unit

  def unload(): Unit

  def servable(): Any
}
