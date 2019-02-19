package com.tencent.angel.serving.core

import org.slf4j.{Logger, LoggerFactory}


class Retry(maxNumRetries: Int, retryIntervalMicros: Long) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Retry])
  private var _numTries: Int = 0

  LOG.info(s"Retry Info: <$maxNumRetries, $retryIntervalMicros>")

  def apply(retriedFn: () => Boolean, isCancelledFn: () => Boolean): Boolean = {
    var isSuccess = false
    var numTries: Int = 0

    LOG.info("Begin to retry")
    do {
      if (numTries > 0) {
        LOG.info(s"[Retry] sleep $retryIntervalMicros --> start")
        Thread.sleep(retryIntervalMicros)
        LOG.info(s"[Retry] sleep $retryIntervalMicros --> finished")
      }
      LOG.info(s"[Retry] call function: retriedFn")
      isSuccess = retriedFn()
      numTries += 1
      _numTries = numTries
      LOG.info(s"\t --> The $numTries-th retry finished!")
    } while (!isCancelledFn() && !isSuccess && numTries <= maxNumRetries)

    LOG.info("End retry")
    isSuccess
  }

  def getNumRetries: Int ={
    _numTries
  }
}


object ConnectSourceToTarget {
  def apply[T](source: Source[T], target: Target[T]): Unit = {
    source.setAspiredVersionsCallback(target.getAspiredVersionsCallback)
  }
}
