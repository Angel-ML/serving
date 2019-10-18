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
package com.tencent.angel.serving.core.metrics

import com.tencent.angel.serving.core.ServableStateMonitor.ServableStateNotifierFn

class MetricsManager {
  def create(target: String, enableMetricSummary: Boolean,
                      metricSummaryWaitSeconds: Int, countDistributionBucket: String): MetricsManager ={
    var metricsCollector: MetricsCollector = null
    if(target.equals("logger")) {
      metricsCollector = new MetricLogger
    } else if(target.equals("syslog")) {
      metricsCollector = new MetricSyslog
    }
    new PredictMetricsManager(metricsCollector, enableMetricSummary, metricSummaryWaitSeconds, countDistributionBucket)
  }

  def createNotifier(elapsedPredictTime: Long, resultStatus: String,
    modelName: String, modelVersion: Long): ServableStateNotifierFn = null

  def getMetricsResult(): String = null

}
