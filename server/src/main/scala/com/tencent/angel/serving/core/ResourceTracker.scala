package com.tencent.angel.serving.core

class ResourceTracker(val totalResources: ResourceAllocation, maxNumLoadRetries: Int, loadRetryIntervalMicros: Long) {
  val usedResources: ResourceAllocation = null
  val retry = new Retry(maxNumLoadRetries, loadRetryIntervalMicros)

  def reserveResources(harness: LoaderHarness): Boolean = synchronized(this) {
    val retriedFn = () => {
      val resources = harness.loader.estimateResources()
      verifyValidity(resources)
      if (overbind(usedResources) + resources < totalResources) {
        true
      } else {
        false
      }
    }

    val isCancelledFn = () => {
      harness.isRetryCanceled
    }

    retry(retriedFn, isCancelledFn)
  }

  def recomputeUsedResources(servables: List[LoaderHarness]): Unit = synchronized(this) {
    usedResources.clear()
    servables.foreach { harness =>
      val resources = harness.loader.estimateResources()
      verifyValidity(resources)
      usedResources += resources
    }
  }

  private def verifyValidity(servableResources: ResourceAllocation): Boolean = {

  }

  private def overbind(servableResources: ResourceAllocation): ResourceAllocation = {

  }
}


