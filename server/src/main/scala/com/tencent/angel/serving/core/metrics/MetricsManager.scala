package com.tencent.angel.serving.core.metrics

import com.tencent.angel.serving.core.ServableStateMonitor.ServableStateNotifierFn

class MetricsManager {
  def create(target: String, enableMetricSummary: Boolean,
                      metricSummaryWaitSeconds: Int): MetricsManager ={
    var metricsCollector: MetricsCollector = null
    if(target.equals("logger")) {
      metricsCollector = new MetricLogger
    } else if(target.equals("syslog")) {
      metricsCollector = new MetricSyslog
    }
    new PredictMetricsManager(metricsCollector, enableMetricSummary, metricSummaryWaitSeconds)
  }

  def createNotifier(elapsedPredictTime: Long, resultStatus: String,
    modelName: String, modelVersion: Long): ServableStateNotifierFn = null
}
