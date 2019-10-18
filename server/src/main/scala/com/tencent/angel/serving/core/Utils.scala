/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.serving.core

import org.slf4j.{Logger, LoggerFactory}


class Retry(maxNumRetries: Int, retryIntervalMicros: Long) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Retry])

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
      LOG.info(s"\t --> The $numTries-th retry finished!")
    } while (!isCancelledFn() && !isSuccess && numTries <= maxNumRetries)

    LOG.info("End retry")
    isSuccess
  }
}


object ConnectSourceToTarget {
  def apply[T](source: Source[T], target: Target[T]): Unit = {
    source.setAspiredVersionsCallback(target.getAspiredVersionsCallback)
  }
}
