package com.tencent.angel.serving.core.metrics

import org.slf4j.{Logger, LoggerFactory}

class MetricsCollector {
  protected val LOG: Logger = LoggerFactory.getLogger(classOf[MetricsCollector])
  def publishMetric(metric: Metric) = {}
}

class MetricLogger extends MetricsCollector {
  override def publishMetric(metric: Metric): Unit ={
    LOG.info(metric.debugString.toString)
  }
}

class MetricSyslog extends MetricsCollector {
  override def publishMetric(metric: Metric): Unit ={
    System.out.println(metric.debugString.toString)
  }
}
