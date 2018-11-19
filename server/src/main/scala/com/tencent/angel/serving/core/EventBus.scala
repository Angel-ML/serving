package com.tencent.angel.serving.core

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ListBuffer


class EventBus[E] {
  import EventBus._

  val lock = new ReentrantLock()

  val subscriptions: ListBuffer[(Subscription[E], Callback[E])] = new ListBuffer[(Subscription[E], Callback[E])]()

  def subscribe(callback: Callback[E]): Subscription[E] = {
    lock.lock()

    try {
      val subscription = new Subscription[E](this)
      subscriptions.append((subscription, callback))
      subscription
    } finally {
      lock.unlock()
    }
  }

  def unsubscribe(subscription: Subscription[E]): Unit = {
    lock.lock()

    try {
      // maybe here is a bug
      subscriptions.zipWithIndex.collect{ case ((sub, _), idx) if sub == subscription => idx }
        .foreach{ idx => subscriptions.remove(idx)}
    } finally {
      lock.unlock()
    }
  }

  def publish(event: E): Unit = {
    lock.lock()

    try {
      val eventAndTime = EventAndTime(event, System.currentTimeMillis())
      subscriptions.foreach{ case (_, callback) => callback(eventAndTime)}
    } finally  {
      lock.unlock()
    }
  }
}

object EventBus {
  case class Subscription[E](bus: EventBus[E])

  case class EventAndTime[E](event: E, eventTimeMicros: Long)

  type Callback[E] = EventAndTime[E] => Unit
}
