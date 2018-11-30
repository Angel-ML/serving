package com.tencent.angel.serving.service

case class Options(
                   port: Int = 8500,
                   rest_api_port: Int = 0,
                   rest_api_timeout_in_ms: Int = 3000,
                   enable_batching: Boolean = true,
                   batching_parameters_file: String = "",
                   model_config_file: String = "",
                   platform_config_file: String = "",
                   model_name: String = "",
                   model_base_path: String = "",
                   saved_model_tags: String = "",
                   max_num_load_retries: Int = 5,
                   load_retry_interval_micros: Long = 60*1000*1000,
                   file_system_poll_wait_seconds: Int = 1,
                   flush_filesystem_caches: Boolean = true,
                   enable_model_warmup: Boolean = true,
                   monitoring_config_file: String = ""
                 ) extends AbstractOptions[Options]