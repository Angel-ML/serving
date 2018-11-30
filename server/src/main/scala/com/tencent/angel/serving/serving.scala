package com.tencent.angel.serving

import com.tencent.angel.config.{FileSystemStoragePathSourceConfigProtos, ModelServerConfigProtos}
import com.tencent.angel.servable.SessionBundleConfigProtos


package object serving {

  type FileSystemStoragePathSourceConfig = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig
  type ServableToMonitor = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableToMonitor

  type ModelServerConfig = ModelServerConfigProtos.ModelServerConfig
  type SessionBundleConfig = SessionBundleConfigProtos.SessionBundleConfig
  type BatchingParameters = SessionBundleConfigProtos.BatchingParameters
}
