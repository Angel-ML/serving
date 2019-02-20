package com.tencent.angel.serving.service.util

case class Options(
                   grpc_port: Int = 8500,
                   http_port: Int = 0,
                   http_timeout_in_ms: Int = 3000,
                   enable_batching: Boolean = true,
                   batching_parameters_file: String = "",
                   model_config_file: String = "",
                   platform_config_file: String = "",
                   model_name: String = "default",
                   model_base_path: String = "",
                   model_platform: String = "angel",
                   hadoop_home: String = "",
                   saved_model_tags: String = "serve",
                   max_num_load_retries: Int = 5,
                   load_retry_interval_micros: Long = 60*1000*1000,
                   file_system_poll_wait_seconds: Int = 1,
                   flush_filesystem_caches: Boolean = true,
                   enable_model_warmup: Boolean = true,
                   monitoring_config_file: String = "",
                   metric_summary_wait_seconds: Int = 30,
                   count_distribution_bucket: String = "5,10,15",
                   enable_metric_summary: Boolean = true,
                   target_publishing_metric: String = "logger"
                 ) extends AbstractOptions[Options]
