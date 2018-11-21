package com.tencent.angel.confg

import com.tencent.angel.config.FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig

class TestClass {
  def test(): FileSystemStoragePathSourceConfig = {
    val sourceConfig = FileSystemStoragePathSourceConfig.newBuilder()
    val servableToMonitor = FileSystemStoragePathSourceConfig.ServableToMonitor.newBuilder()
    sourceConfig.addServables(servableToMonitor.build())

    sourceConfig.build()
  }
}
