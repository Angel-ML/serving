package com.tencent.angel.serving.core


class Retry(maxNumRetries: Int, retryIntervalMicros: Long) {
  def apply(retriedFn: () => Boolean, isCancelledFn: () => Boolean): Boolean = {
    var isSuccess = false
    var numTries: Int = 0
    do {
      if (numTries > 0) {
        Thread.sleep(retryIntervalMicros)
      }
      isSuccess = retriedFn()
      numTries += 1
    } while (!isCancelledFn() && !isSuccess && numTries <= maxNumRetries)

    isSuccess
  }
}


object ConnectSourceToTarget {
  def apply[T](source: Source[T], target: Target[T]): Unit = {
    source.setAspiredVersionsCallback(target.getAspiredVersionsCallback)
  }
}
