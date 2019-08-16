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
package com.tencent.angel.serving.sources

import java.net.URI
import org.apache.hadoop.conf.Configuration
import scala.collection.immutable.Set
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.matching.Regex
import com.tencent.angel.serving.core.{FailedPreconditions, NotFoundExceptions}
import com.tencent.angel.serving.service.ModelServer

object SystemFileUtils {
  private var fileSystem = null.asInstanceOf[FileSystem]
  def getFileSystem(conf: Configuration = ModelServer.hadoopConf): FileSystem ={
    if (fileSystem == null) {
      this.synchronized {
        if (fileSystem == null) {
          try {
            if (conf == null) {
              throw FailedPreconditions("hadoop configuration has not been set!")
            }
            fileSystem = FileSystem.get(conf)
          } catch {
            case e: Exception =>
              e.printStackTrace()
          }
        }
      }
    }
    fileSystem
  }

  def dirExist(basePath: String, conf: Configuration = ModelServer.hadoopConf): Boolean ={
    val fs = getFileSystem(conf)
    if (fs.exists(new Path(basePath))) {
      fs.isDirectory(new Path(basePath))
    } else {
      throw NotFoundExceptions(s"the basePath: ${basePath} is not found!")
    }
  }

  def fileExist(basePath: String, conf: Configuration = ModelServer.hadoopConf): Boolean ={
    val fs = getFileSystem(conf)
    fs.exists(new Path(basePath))
  }

  def getTotalSpace(basePath: String, conf: Configuration = ModelServer.hadoopConf): Long ={
    val fs = getFileSystem(conf)
    fs.getContentSummary(new Path(basePath)).getLength
  }

  def getChildren(basePath: String, conf: Configuration = ModelServer.hadoopConf):Set[String] = {
    val fs = getFileSystem(conf)
    val path = new Path(basePath)
    val fileStatus  = fs.listStatus(path)
    val pattern = new Regex("^[0-9]*$")
    val children = fileStatus.filter(fs => fs.isDirectory).filterNot(fs => fs.getPath.equals(path)).map{ fs =>
      pattern.findAllMatchIn(fs.getPath.getName).mkString("")
    }.filterNot(child => child.isEmpty)
    children.toSet
  }
}

