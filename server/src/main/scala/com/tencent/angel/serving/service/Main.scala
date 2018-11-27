package com.tencent.angel.serving.service

import com.tencent.angel.serving.service.ModelServer.server
import scopt.OptionParser

object Main {

  case class Params(
                     port: Int = 8500,
                     rest_api_port: Int = 0,
                     rest_api_timeout_in_ms: Int = 0,
                     enable_batching: Boolean = true,
                     batching_parameters_file: String = "",
                     model_config_file: String = "",
                     model_name: String = "",
                     model_base_path: String = "",
                     max_num_load_retries: Int = 0,
                     load_retry_interval_micros: Long = 0,
                     file_system_poll_wait_seconds: Int = 0,
                     flush_filesystem_caches: Boolean = true,
                     enable_model_warmup: Boolean = true,
                     monitoring_config_file: String = ""
                   ) extends AbstractParams[Params]

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("ModelServer") {

      opt[Int]("port")
        .text("Port to listen on for gRPC API")
        .required()
        .action((x, c) => c.copy(port = x))
      opt[Int]("rest_api_port")
        .text("Port to listen on for HTTP/REST API.")
        .required()
        .action((x, c) => c.copy(rest_api_port = x))
      opt[Int]("rest_api_timeout_in_ms")
        .text("Timeout for HTTP/REST API calls.")
        .action((x, c) => c.copy(rest_api_timeout_in_ms = x))
      opt[Boolean]("enable_batching")
        .text("enable batching.")
        .action((x, c) => c.copy(enable_batching = x))
      opt[String]("batching_parameters_file")
        .text("")
        .action((x, c) => c.copy(batching_parameters_file = x))
      opt[String]("model_config_file")
        .text("")
        .action((x, c) => c.copy(model_config_file = x))
      opt[String]("model_name")
        .text("")
        .action((x, c) => c.copy(model_name = x))
      opt[String]("model_base_path")
        .text("")
        .action((x, c) => c.copy(model_base_path = x))
      opt[Int]("max_num_load_retries")
        .text("")
        .action((x, c) => c.copy(max_num_load_retries = x))
      opt[Long]("load_retry_interval_micros")
        .text("")
        .action((x, c) => c.copy(load_retry_interval_micros = x))
      opt[Int]("file_system_poll_wait_seconds")
        .text("")
        .action((x, c) => c.copy(file_system_poll_wait_seconds = x))
      opt[Boolean]("flush_filesystem_caches")
        .text("")
        .action((x, c) => c.copy(flush_filesystem_caches = x))
      opt[Boolean]("enable_model_warmup")
        .text("")
        .action((x, c) => c.copy(enable_model_warmup = x))
      opt[String]("monitoring_config_file")
        .text("")
        .action((x, c) => c.copy(monitoring_config_file = x))
    }
    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params): Unit ={
    val options = Options()
    options.grpcPort = params.port
    options.httpPort = params.rest_api_port
    options.httpTimeoutInMs = params.rest_api_timeout_in_ms
    options.enableBatching = params.enable_batching
    options.batchingParametersFile = params.batching_parameters_file
    options.modelConfigFile = params.model_config_file
    options.monitoringConfigFile = params.monitoring_config_file
    options.model_name = params.model_name
    options.modelBasePath = params.model_base_path
    options.maxNumLoadRetries = params.max_num_load_retries
    options.loadRetryIntervalMicros = params.load_retry_interval_micros
    options.fileSystemPollWaitSeconds = params.file_system_poll_wait_seconds
    options.flushFilesystemCaches = params.flush_filesystem_caches
    options.enableModelWarmup = params.enable_model_warmup
    server = new ModelServer
    server.buildAndStart(options)
    server.waitForTermination()
  }
}
