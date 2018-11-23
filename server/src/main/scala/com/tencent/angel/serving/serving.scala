package com.tencent.angel.serving

import com.tencent.angel.config.{FileSystemStoragePathSourceConfigProtos, ModelServerConfigProtos}


package object serving {

  type FileSystemStoragePathSourceConfig = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig

  type ModelServerConfig = ModelServerConfigProtos.ModelServerConfig
}
