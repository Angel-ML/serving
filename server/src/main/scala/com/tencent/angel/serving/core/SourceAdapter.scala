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

import java.util.concurrent.locks.{Condition, ReentrantLock}

abstract class SourceAdapter[S, T] extends TargetBase[T] with Source[S] {
  protected var outgoingCallback: AspiredVersionsCallback[S] = _
  protected var flag: Boolean = false
  val lock: ReentrantLock = new ReentrantLock
  val cond: Condition = lock.newCondition()

  def adapt(servableName: String, versions: List[ServableData[T]]): List[ServableData[S]]

  def adaptOneVersion(version: ServableData[T]): ServableData[S] = {
    adapt(version.id.name, List[ServableData[T]](version)).head
  }

  def setAspiredVersionsCallback(callback: AspiredVersionsCallback[S]): Unit = {
    lock.lock()
    try {
      outgoingCallback = callback
      flag = true
      cond.signal()
    } finally {
      lock.unlock()
    }
  }

  def setAspiredVersions(servableName: String, versions: List[ServableData[T]]): Unit = {
    lock.lock()
    try {
      while (!flag) {
        cond.await()
      }

      outgoingCallback(servableName, adapt(servableName, versions))
    } finally {
      lock.unlock()
    }
  }
}


abstract class UnarySourceAdapter[S, T] extends SourceAdapter[S, T] {
  def convert(data: ServableData[T]): ServableData[S]

  override def adapt(servableName: String, versions: List[ServableData[T]]): List[ServableData[S]] = {
    versions.map { version =>
      val adapted = convert(version)
      if (adapted != null) {
        adapted
      } else {
        new ServableData[S](version.id, null.asInstanceOf[S])
      }

    }
  }
}


class ErrorSourceAdapter[S, T](exception: Exception) extends SourceAdapter[S, T] {
  override def adapt(servableName: String, versions: List[ServableData[T]]): List[ServableData[S]] = {
    versions.map { version => new ServableData[S](version.id, null.asInstanceOf[S]) }
  }
}


class IdentitySourceAdapter[T] extends SourceAdapter[T, T] {
  override def adapt(servableName: String, versions: List[ServableData[T]]): List[ServableData[T]] = versions
}

