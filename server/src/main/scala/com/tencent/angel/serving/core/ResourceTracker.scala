package com.tencent.angel.serving.core

import com.tencent.angel.confg.ResourceAllocation

class ResourceTracker(val totalResources: ResourceAllocation, maxNumLoadRetries: Int, loadRetryIntervalMicros: Long) {
  val usedResources: ResourceAllocation = null
  val retry = new Retry(maxNumLoadRetries, loadRetryIntervalMicros)

  def reserveResources(harness: LoaderHarness): Boolean = synchronized(this) {
    val retriedFn = () => {
      val resources = harness.loader.estimateResources()
      if (resources.verify()) {
        if (overbind(usedResources) + resources < totalResources) {
          true
        } else {
          false
        }
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
      if (resources.verify()) {
        usedResources += resources
      } else {
        // resource error
        throw ResourceExceptions("estimateResources Error!")
      }
    }
  }


  private def overbind(servableResources: ResourceAllocation): ResourceAllocation = servableResources * 1.2
}


