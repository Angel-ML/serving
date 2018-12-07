package com.tencent.angel.serving.core

import com.tencent.angel.config.SamplingConfigProtos

class ServerRequestLogger {
  def update(loggingMap: Map[String, SamplingConfigProtos.LoggingConfig]): Unit= {}
}
