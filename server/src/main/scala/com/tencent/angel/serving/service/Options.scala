package com.tencent.angel.serving.service

case class Options() {
  var grpcPort: Int = 8500

  var httpPort: Int = 0
  var httpTimeoutInMs = 30000

  var enableBatching: Boolean = false
  var batchingParametersFile: String = _
  var model_name: String = _
  var maxNumLoadRetries: Int = 5
  var loadRetryIntervalMicros: Long = 1L * 60 * 1000 * 1000
  var fileSystemPollWaitSeconds: Int = 1
  var flushFilesystemCaches: Boolean = true
  var modelBasePath: String = _
  var savedModelTags: String = _
  var platformConfigFile: String = _
  var modelConfigFile: String = _
  var enableModelWarmup: Boolean = true
  var monitoringConfigFile: String = _
}
