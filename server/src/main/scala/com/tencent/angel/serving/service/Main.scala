package com.tencent.angel.serving.service

import com.tencent.angel.serving.service.ModelServer.server
import com.tencent.angel.serving.service.util.Options
import scopt.OptionParser

object Main {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val defaultOptions = Options()
    val parser = new OptionParser[Options]("ModelServer") {

      opt[Int]("port")
        .text("Port to listen on for gRPC API")
        .required()
        .action((x, c) => c.copy(grpc_port = x))
      opt[Int]("rest_api_port")
        .text("Port to listen on for HTTP/REST API.If set to zero " +
          "HTTP/REST API will not be exported. This port must be " +
          "different than the one specified in --port.")
        .required()
        .action((x, c) => c.copy(http_port = x))
      opt[Int]("rest_api_timeout_in_ms")
        .text("Timeout for HTTP/REST API calls.")
        .action((x, c) => c.copy(http_timeout_in_ms = x))
      opt[Boolean]("enable_batching")
        .text("enable batching.")
        .action((x, c) => c.copy(enable_batching = x))
      opt[String]("batching_parameters_file")
        .text("If non-empty, read an ascii BatchingParameters " +
          "protobuf from the supplied file name and use the " +
          "contained values instead of the defaults.")
        .action((x, c) => c.copy(batching_parameters_file = x))
      opt[String]("model_config_file")
        .text("If non-empty, read an ascii ModelServerConfig " +
          "protobuf from the supplied file name, and serve the " +
          "models in that file. This config file can be used to " +
          "specify multiple models to serve and other advanced " +
          "parameters including non-default version policy. (If " +
          "used, --model_name, --model_base_path are ignored.)" +
          "add the prefix `file:///` to model_base_path when use localfs")
        .action((x, c) => c.copy(model_config_file = x))
      opt[String]("hadoop_home")
        .text("hadoop_home " +
          "which contains hdfs-site.xml and core-site.xml.")
        .action((x, c) => c.copy(hadoop_home = x))
      opt[String]("model_name")
        .text("name of model (ignored " +
          "if --model_config_file flag is set")
        .action((x, c) => c.copy(model_name = x))
      opt[String]("model_base_path")
        .text("path to export (ignored if --model_config_file flag " +
          "is set, otherwise required), " +
          "add the prefix `file:///` to model_base_path when use localfs")
        .action((x, c) => c.copy(model_base_path = x))
      opt[String]("model_platform")
        .text("platform for model serving (ignored if --model_config_file flag " +
          "is set, otherwise required), ")
        .action((x, c) => c.copy(model_platform = x))
      opt[String]("saved_model_tags")
        .text("Comma-separated set of tags corresponding to the meta " +
          "graph def to load from SavedModel.")
        .action((x, c) => c.copy(saved_model_tags = x))
      opt[Int]("max_num_load_retries")
        .text("maximum number of times it retries loading a model " +
          "after the first failure, before giving up. " +
          "If set to 0, a load is attempted only once. " +
          "Default: 5")
        .action((x, c) => c.copy(max_num_load_retries = x))
      opt[Long]("load_retry_interval_micros")
        .text("The interval, in microseconds, between each servable " +
          "load retry. If set negative, it doesn't wait. " +
          "Default: 1 minute")
        .action((x, c) => c.copy(load_retry_interval_micros = x))
      opt[Int]("file_system_poll_wait_seconds")
        .text("interval in seconds between each poll of the file " +
          "system for new model version")
        .action((x, c) => c.copy(file_system_poll_wait_seconds = x))
      opt[Boolean]("flush_filesystem_caches")
        .text("If true (the default), filesystem caches will be " +
          "flushed after the initial load of all servables, and " +
          "after each subsequent individual servable reload (if " +
          "the number of load threads is 1). This reduces memory " +
          "consumption of the model server, at the potential cost " +
          "of cache misses if model files are accessed after " +
          "servables are loaded.")
        .action((x, c) => c.copy(flush_filesystem_caches = x))
      opt[Boolean]("enable_model_warmup")
        .text("Enables model warmup, which triggers lazy " +
          "initializations (such as TF optimizations) at load " +
          "time, to reduce first request latency.")
        .action((x, c) => c.copy(enable_model_warmup = x))
      opt[String]("monitoring_config_file")
        .text("If non-empty, read an ascii MonitoringConfig protobuf from " +
          "the supplied file name")
        .action((x, c) => c.copy(monitoring_config_file = x))
      opt[String]("metric_implementation")
        .text("Defines the implementation of the metrics to be used (logger, " +
          "syslog ...). ")
        .action((x, c) => c.copy(target_publishing_metric = x))
      opt[Boolean]("enable_metric_summary")
        .text("Enable summary for metrics, launch an async task.")
        .action((x, c) => c.copy(enable_metric_summary = x))
      opt[String]("count_distribution_bucket")
        .text("response time interval distribution.")
        .action((x, c) => c.copy(count_distribution_bucket = x))
      opt[String]("hadoop_job_ugi")
        .text("hadoop job ugi to access hdfs, Separated by commas, example:\"test, test\".")
        .action((x, c) => c.copy(hadoop_job_ugi = x))
      opt[Int]("metric_summary_wait_seconds")
        .text("Interval in seconds between each summary of metrics." +
          "(Ignored if --enable_metric_summary=false)")
        .action((x, c) => c.copy(metric_summary_wait_seconds = x))
    }
    parser.parse(args, defaultOptions).map { options =>
      run(options)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(options: Options): Unit ={
    server = new ModelServer
    server.buildAndStart(options)
    server.waitForTermination()
  }
}
