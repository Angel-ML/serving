package com.tencent.angel.serving.core.metrics

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.tencent.angel.serving.core.ManagerState.ManagerState
import com.tencent.angel.serving.core.{ManagerState, ServableId}
import com.tencent.angel.serving.core.ServableStateMonitor.ServableStateNotifierFn

import scala.collection.mutable

class PredictMetricsManager(metricsCollector: MetricsCollector, enableMetricSummary: Boolean,
                            metricSummaryWaitSeconds: Int = 0) extends MetricsManager {
  private val _metricsCollector: MetricsCollector = metricsCollector
  private val _metricsMap = new mutable.HashMap[ServableId, PredictMetric]()
  private val _summaryMetrics = new mutable.HashMap[String, PredictMetricSummary]()
  private val _countDistribution = new mutable.HashMap[String, Long]()
  private var n0Count: Long = 0
  private var n1Count: Long = 0
  private var n2Count: Long = 0
  private var n3Count: Long = 0
  private var successPredictCount: Long = 0
  private var failedPredictCount: Long = 0
  private var predictCount: Long = 0
  private val _metricSummaryWaitSeconds: Int = metricSummaryWaitSeconds
  private val _summaryThreadRunning = new AtomicBoolean(true)
  private val executorService = Executors.newSingleThreadExecutor()
  try {
    executorService.execute(new Runnable {
      override def run(): Unit = {
        while(_summaryThreadRunning.get()) {
          createSummaryMetric()
          if(enableMetricSummary) {
            _summaryMetrics.foreach { case (_, v) =>
              _metricsCollector.publishMetric(v)
            }
          }
          _metricsMap.clear()
          Thread.sleep(_metricSummaryWaitSeconds * 1000)
        }
      }
    })
  } catch {
    case ex: Exception =>
      ex.printStackTrace()
      killSummaryThread()
    case _ =>
      killSummaryThread()
      println("Summary Thread Running exception.")
  }

  def createSummaryMetric(): Unit = {
    _metricsMap.foreach { case (_, predictMetric) =>
      val successSummaryKey: String = predictMetric._modelName + predictMetric._modelVersion + "_success"
      val failedSummaryKey: String = predictMetric._modelName + predictMetric._modelVersion + "_failed"
      var summaryKey: String = ""
      if(predictMetric._isSucess) {
        summaryKey = successSummaryKey
        successPredictCount = successPredictCount + 1
        predictCount = successPredictCount
        val predictTimeMs = predictMetric._predictTimeMs
        if(predictTimeMs >= 0 && predictTimeMs <=5) {
          n0Count = n0Count + 1
        } else if(predictTimeMs > 5 && predictTimeMs <= 10) {
          n1Count = n1Count + 1
        } else if(predictTimeMs > 10 && predictTimeMs <= 15) {
          n2Count = n2Count + 1
        } else {
          n3Count = n3Count + 1
        }
      } else {
        summaryKey = failedSummaryKey
        failedPredictCount = failedPredictCount + 1
        predictCount = failedPredictCount
      }
      if(_summaryMetrics.contains(summaryKey)) {
        val metric = _summaryMetrics(summaryKey)
        val accumuPredictTimesMs:Long = metric._accumuPredictTimesMs + predictMetric._predictTimeMs
        val averagePredictTimeMs: Double = accumuPredictTimesMs.toDouble / predictCount.toDouble
        _summaryMetrics(summaryKey) = new PredictMetricSummary("PredictSummary", predictCount,
          averagePredictTimeMs, predictMetric._modelName, predictMetric._modelVersion, predictMetric._isSucess,
          _metricSummaryWaitSeconds, accumuPredictTimesMs)
      } else {
        _summaryMetrics(summaryKey) = new PredictMetricSummary("PredictSummary", 1,
          predictMetric._predictTimeMs, predictMetric._modelName, predictMetric._modelVersion, predictMetric._isSucess,
          _metricSummaryWaitSeconds, predictMetric._predictTimeMs)
      }
    }
    _countDistribution("0-5ms") = n0Count
    _countDistribution("5-10ms") = n1Count
    _countDistribution("10-15ms") = n2Count
    _countDistribution("15+ms") = n3Count
  }

  def killSummaryThread(): Unit = {
    _summaryThreadRunning.set(false)
    executorService.shutdown()
  }

  override def getMetricsResult(): String ={
    val summaryMetricsResult = new mutable.HashMap[String, String]()
    _summaryMetrics.foreach{case (summaryKey, predictMetricSummary) =>
      summaryMetricsResult(summaryKey) = predictMetricSummary.debugString
    }
    if(summaryMetricsResult.isEmpty) {
      summaryMetricsResult("Info") = "There is no summary metrics."
    }
    scala.util.parsing.json.JSONObject(summaryMetricsResult.toMap).toString().replace("\\", "")
  }

  override def getResponseTimeDistributionResult(): String = {
    "{ response_time_distribution: " +
      scala.util.parsing.json.JSONObject(_countDistribution.toMap).toString().replace("\\", "") +
      " }"
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
              if(enableMetricSummary) {
                _metricsCollector.publishMetric(metric)
              }
          }

      }
    }
    notifierFn
  }

}
