package com.tencent.angel.serving.core.metrics

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.tencent.angel.serving.core.ManagerState.ManagerState
import com.tencent.angel.serving.core.{ManagerState, ServableId}
import com.tencent.angel.serving.core.ServableStateMonitor.ServableStateNotifierFn

import scala.collection.mutable

class PredictMetricsManager(metricsCollector: MetricsCollector, enableMetricSummary: Boolean,
                            metricSummaryWaitSeconds: Int = 0) extends MetricsManager {
  private var _metricsCollector: MetricsCollector = metricsCollector
  private val _metricsMap = new mutable.HashMap[ServableId, PredictMetric]()
  private val _summaryMetrics = new mutable.HashMap[String, PredictMetricSummary]()
  private var _metricSummaryWaitSeconds: Int = metricSummaryWaitSeconds
  private val _summaryThreadRunning = new AtomicBoolean(true)
  private val executorService = Executors.newSingleThreadExecutor()
  if(enableMetricSummary) {
    executorService.execute(new Runnable {
      override def run(): Unit = {
        while(_summaryThreadRunning.get()) {
          createSummaryMetric(_metricsMap, _summaryMetrics)
          _summaryMetrics.foreach { case (_, v) =>
            _metricsCollector.publishMetric(v)
          }
          _metricsMap.clear()
          _summaryMetrics.clear()
          Thread.sleep(_metricSummaryWaitSeconds * 1000)
        }
      }
    })
  }

  def createSummaryMetric(metricsMap: mutable.HashMap[ServableId, PredictMetric],
                          summaryMetrics: mutable.HashMap[String, PredictMetricSummary]): Unit = {
    var successPredictCount: Long = 0
    var failedPredictCount: Long = 0
    var predictCount: Long = 0
    metricsMap.foreach { case (_, predictMetric) =>
        val successSummaryKey: String = predictMetric._modelName + predictMetric._modelVersion + "_success"
        val failedSummaryKey: String = predictMetric._modelName + predictMetric._modelVersion + "_failed"
        var summaryKey: String = ""
        if(predictMetric._isSucess) {
          summaryKey = successSummaryKey
          successPredictCount = successPredictCount + 1
          predictCount = successPredictCount
        } else {
          summaryKey = failedSummaryKey
          failedPredictCount = failedPredictCount + 1
          predictCount = failedPredictCount
        }
        if(summaryMetrics.contains(summaryKey)) {
          val metric = summaryMetrics(summaryKey)
          val accumuPredictTimesMs:Long = metric._accumuPredictTimesMs + predictMetric._predictTimeMs
          val averagePredictTimeMs: Long = accumuPredictTimesMs / predictCount
          summaryMetrics(summaryKey) = new PredictMetricSummary("PredictSummary", predictCount,
            averagePredictTimeMs, predictMetric._modelName, predictMetric._modelVersion, predictMetric._isSucess,
            _metricSummaryWaitSeconds, accumuPredictTimesMs)
        } else {
          summaryMetrics(summaryKey) = new PredictMetricSummary("PredictSummary", 1,
            predictMetric._predictTimeMs, predictMetric._modelName, predictMetric._modelVersion, predictMetric._isSucess,
            _metricSummaryWaitSeconds, predictMetric._predictTimeMs)
        }
    }
  }

  def killSummaryThread(): Unit = {
    _summaryThreadRunning.set(false)
    executorService.shutdown()
  }

  override def createNotifier(elapsedPredictTime: Long, resultStatus: String,
                              modelName: String, modelVersion: Long): ServableStateNotifierFn = {
    val notifierFn: ServableStateNotifierFn = (statesReached: Map[ServableId, ManagerState]) => {
      statesReached.map { case (servableId, managerState) =>
          managerState match {
            case ManagerState.kStart | ManagerState.kLoading | ManagerState.kUnloading | ManagerState.kAvailable => null
            case ManagerState.kEnd =>
              val metric = new PredictMetric(servableId.name, servableId.version,
                elapsedPredictTime, modelName, modelVersion, resultStatus.equals("ok"))
              _metricsMap(servableId) = metric
              _metricsCollector.publishMetric(metric)
          }

      }
    }
    notifierFn
  }

}
