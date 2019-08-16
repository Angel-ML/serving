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

trait Target[T] {
  def getAspiredVersionsCallback: AspiredVersionsCallback[T]
}


abstract class TargetBase[T] extends Target[T] {
  protected var detached: Boolean = false

  def setAspiredVersions(servableName: String, versions: List[ServableData[T]]): Unit

  def detach(): Unit = {
    if (!detached) {
      detached = true
    } else {
      throw new Exception("detach() must be called exactly once")
    }
  }

  def getAspiredVersionsCallback: AspiredVersionsCallback[T] = {
    if (detached) {
      (name: String, versions: List[ServableData[T]]) => {}
    } else {
      (name: String, versions: List[ServableData[T]]) => {
        setAspiredVersions(name, versions)
      }
    }
  }
}
