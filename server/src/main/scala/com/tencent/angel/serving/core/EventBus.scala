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

import java.util
import java.util.concurrent.locks.ReentrantLock


class EventBus[E] {

  import EventBus._

  private val lock = new ReentrantLock()

  private val subscriptions: util.ArrayList[(Subscription[E], Callback[E])] = new util.ArrayList[(Subscription[E], Callback[E])]()

  def subscribe(callback: Callback[E]): Subscription[E] = {
    lock.lock()

    try {
      val subscription = new Subscription[E](this)
      subscriptions.add((subscription, callback))
      subscription
    } finally {
      lock.unlock()
    }
  }

  def unsubscribe(subscription: Subscription[E]): Unit = {
    lock.lock()

    try {
      val iter = subscriptions.iterator()
      while (iter.hasNext) {
        val sub = iter.next()._1
        if (subscription != null && sub == subscription) {
          iter.remove()
        }
      }
    } finally {
      lock.unlock()
    }
  }

  def publish(event: E): Unit = {
    lock.lock()

    try {
      val eventAndTime = EventAndTime(event, System.currentTimeMillis())
      val iter = subscriptions.iterator()
      while (iter.hasNext) {
        val callback = iter.next()._2
        callback(eventAndTime)
      }
    } finally {
      lock.unlock()
    }
  }
}


object EventBus {

  def apply[E]() = new EventBus[E]

  case class Subscription[E](bus: EventBus[E])

  case class EventAndTime[E](event: E, eventTimeMicros: Long) {
    def state: E = event
  }

  type Callback[E] = EventAndTime[E] => Unit
}
