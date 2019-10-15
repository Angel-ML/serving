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

import java.util
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.google.protobuf.util.JsonFormat
import com.tencent.angel.serving.core.ManagerState.ManagerState
import com.tencent.angel.serving.core.{ManagerState, ServableId}
import com.tencent.angel.serving.core.ServableStateMonitor.ServableStateNotifierFn

import scala.collection.mutable

class PredictMetricsManager(metricsCollector: MetricsCollector, enableMetricSummary: Boolean,
                            metricSummaryWaitSeconds: Int = 0, countDistributionBucket: String) extends MetricsManager {
  private val _metricsCollector: MetricsCollector = metricsCollector
  private val _metricsMap = new mutable.HashMap[ServableId, PredictMetric]()
  private val _summaryMetrics = new mutable.HashMap[String, PredictMetricSummary]()
  private val successPredictCountMap = new mutable.HashMap[String, Long]().withDefaultValue(0)
  private val failedPredictCountMap = new mutable.HashMap[String, Long]().withDefaultValue(0)
  private val n0CountMap = new mutable.HashMap[String, Long]().withDefaultValue(0)
  private val n1CountMap = new mutable.HashMap[String, Long]().withDefaultValue(0)
  private val n2CountMap = new mutable.HashMap[String, Long]().withDefaultValue(0)
  private val n3CountMap = new mutable.HashMap[String, Long]().withDefaultValue(0)
  private val _metricSummaryWaitSeconds: Int = metricSummaryWaitSeconds
  private val _countDistributionBucketList = new util.ArrayList[Int]()
  private val _summaryThreadRunning = new AtomicBoolean(true)
  private val executorService = Executors.newSingleThreadExecutor()
  setCountDistributionBucketList(countDistributionBucket)
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

  def setCountDistributionBucketList(countDistributionBucket: String): Unit = {
    try{
      val splits = countDistributionBucket.split(",")
      if(splits.size != 3) {
        throw new Exception("count_distribution_bucket format error.")
      }
      splits.foreach(e => _countDistributionBucketList.add(e.toInt))
    } catch {
      case ex: Exception =>
        println(ex.printStackTrace() + ", Now use count_distribution_bucket default value \"5,10,15\".")
        _countDistributionBucketList.clear()
        Array(5, 10, 15).foreach(e => _countDistributionBucketList.add(e))
    }
  }

  def createSummaryMetric(): Unit = {
    _metricsMap.foreach { case (_, predictMetric) =>
      val summaryKey: String = predictMetric._modelName + "_" + predictMetric._modelVersion
      if(predictMetric._isSucess) {
        successPredictCountMap(summaryKey) = successPredictCountMap(summaryKey) + 1
        val predictTimeMs = predictMetric._predictTimeMs
        if(predictTimeMs >= 0 && predictTimeMs <=_countDistributionBucketList.get(0)) {
          n0CountMap(summaryKey) = n0CountMap(summaryKey) + 1
        } else if(predictTimeMs > _countDistributionBucketList.get(0) && predictTimeMs <= _countDistributionBucketList.get(1)) {
          n1CountMap(summaryKey) = n1CountMap(summaryKey) + 1
        } else if(predictTimeMs > _countDistributionBucketList.get(1) && predictTimeMs <= _countDistributionBucketList.get(2)) {
          n2CountMap(summaryKey) = n2CountMap(summaryKey) + 1
        } else {
          n3CountMap(summaryKey) = n3CountMap(summaryKey) + 1
        }
      } else {
        failedPredictCountMap(summaryKey) = failedPredictCountMap(summaryKey) + 1
      }
      if(_summaryMetrics.contains(summaryKey)) {
        val metric = _summaryMetrics(summaryKey)
        var accumuPredictTimesMs:Long = metric._accumuPredictTimesMs
        if(predictMetric._isSucess) {
          accumuPredictTimesMs = accumuPredictTimesMs + predictMetric._predictTimeMs
        }
        val predictionCountSuccess: Long = successPredictCountMap(summaryKey)
        val predictionCountFailed: Long = failedPredictCountMap(summaryKey)
        val predictCountTotal: Long = predictionCountSuccess + predictionCountFailed
        _summaryMetrics(summaryKey) = new PredictMetricSummary("PredictSummary", predictCountTotal,
          predictionCountSuccess, predictionCountFailed, predictMetric._modelName, predictMetric._modelVersion,
          accumuPredictTimesMs, n0CountMap(summaryKey), n1CountMap(summaryKey), n2CountMap(summaryKey), n3CountMap(summaryKey))
      } else {
        val predictionCountSuccess: Long = successPredictCountMap(summaryKey)
        val predictionCountFailed: Long = failedPredictCountMap(summaryKey)
        val predictCountTotal: Long = predictionCountSuccess + predictionCountFailed
        var accumuPredictTimesMs: Long = 0
        if(predictMetric._isSucess) {
          accumuPredictTimesMs = predictMetric._predictTimeMs
        }
        _summaryMetrics(summaryKey) = new PredictMetricSummary("PredictSummary", predictCountTotal,
          predictionCountSuccess, predictionCountFailed, predictMetric._modelName, predictMetric._modelVersion,
          accumuPredictTimesMs, n0CountMap(summaryKey), n1CountMap(summaryKey), n2CountMap(summaryKey),
          n3CountMap(summaryKey))
      }
    }
  }

  def killSummaryThread(): Unit = {
    _summaryThreadRunning.set(false)
    executorService.shutdown()
  }

  override def getMetricsResult(): String ={
    import com.tencent.angel.metrics.ServingMetricsProtos
    val metricsResponseBuilder = ServingMetricsProtos.MetricsResponse.newBuilder()
    val models = new mutable.HashMap[String, ServingMetricsProtos.MetricsResponse.Versions.Builder]()
    _summaryMetrics.foreach { case (summaryKey, predictMetricSummary) =>
      val metricsBuilder = ServingMetricsProtos.Metrics.newBuilder()
      metricsBuilder.setModelName(predictMetricSummary._modelName)
        .setModelVersion(predictMetricSummary._modelVersion.toInt)
        .setPredictionCountTotal(predictMetricSummary._predictionCountTotal)
        .setPredictionCountSuccess(predictMetricSummary._predictionCountSuccess)
        .setPredictionCountFailed(predictMetricSummary._predictionCountFailed)
        .setTotalPredictTimeMs(predictMetricSummary._accumuPredictTimesMs.toDouble)
        .setCountDistribution0(predictMetricSummary._countDistribution0)
        .setCountDistribution1(predictMetricSummary._countDistribution1)
        .setCountDistribution2(predictMetricSummary._countDistribution2)
        .setCountDistribution3(predictMetricSummary._countDistribution3)
      if(models.contains(predictMetricSummary._modelName)) {
        models(predictMetricSummary._modelName).putVersions(predictMetricSummary._modelVersion.toString, metricsBuilder.build())
      } else {
        val versionsBuilder = ServingMetricsProtos.MetricsResponse.Versions.newBuilder()
        versionsBuilder.putVersions(predictMetricSummary._modelVersion.toString, metricsBuilder.build())
        models.put(predictMetricSummary._modelName, versionsBuilder)
      }
    }
    models.foreach { case (modelName, versionBuilder) =>
        metricsResponseBuilder.putModels(modelName, versionBuilder.build())
    }
    JsonFormat.printer().preservingProtoFieldNames().print(metricsResponseBuilder.build())
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
