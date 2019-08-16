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


import com.google.protobuf.Message
import com.tencent.angel.servable.SavedModelBundleSourceAdapterConfigProtos.SavedModelBundleSourceAdapterConfig
import com.tencent.angel.servable.SessionBundleSourceAdapterConfigProtos.SessionBundleSourceAdapterConfig

object ClassFactory {

  //create an instance of BaseClass based on a config proto
  def create[BaseClass](config: Message, classCreator: String, methodName: String="apply"): BaseClass = {
    val cls = Class.forName(classCreator)
    val result = config match {
      case savedConf: SavedModelBundleSourceAdapterConfig =>
        val method = cls.getDeclaredMethod(methodName, classOf[SavedModelBundleSourceAdapterConfig])
        method.invoke(null, savedConf)
      case sessionConf: SessionBundleSourceAdapterConfig =>
        val method = cls.getDeclaredMethod(methodName, classOf[SessionBundleSourceAdapterConfig])
        method.invoke(null, sessionConf)
    }

    result.asInstanceOf[BaseClass]
  }

  def createFromAny[BaseClass](platform: String, anyConfig: com.google.protobuf.Any): BaseClass = {
    val (config, fullTypeName) = if (anyConfig.is(classOf[SavedModelBundleSourceAdapterConfig])) {
      val congObj = anyConfig.unpack(classOf[SavedModelBundleSourceAdapterConfig])
      congObj -> congObj.getAdapterClassName
    } else {
      val congObj = anyConfig.unpack(classOf[SessionBundleSourceAdapterConfig])
      congObj -> congObj.getAdapterClassName
    }

    create[BaseClass](config, fullTypeName)
  }
}