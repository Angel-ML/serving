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
                   target_publishing_metric: String = "logger",
                   hadoop_job_ugi: String = "",
                   principal: String = "",
                   keytab: String = ""
                 ) extends AbstractOptions[Options]
