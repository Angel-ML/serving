package com.tencent.angel.serving.core.metrics

class Metric(metricName: String, metricVersion: Long = 0, modelName: String, modelVersion: Long = 0) {
  var _metricName: String = metricName
  var _metricVersion: Long = metricVersion
  var _modelName: String = modelName
  var _modelVersion: Long = modelVersion

  def debugString: String = null

}

class PredictMetric(metricName: String, metricVersion: Long, predictTimeMs: Long = 0,
                    modelName: String, modelVersion: Long, isSucess: Boolean = false)
  extends Metric(metricName, metricVersion, modelName, modelVersion) {
  var _predictTimeMs: Long = predictTimeMs
  var _isSucess: Boolean = isSucess

  override def debugString: String ={
    "metric_name=\"" + _metricName + "\", " + "metric_version=" + _metricVersion + ", " +
    "model_name=\"" + _modelName + "\", " + "model_version=" + _modelVersion + ", " + "is_success=" +
    _isSucess + ", " + "predict_time_ms=" + _predictTimeMs
  }
}

class PredictMetricSummary(metricName: String, predictionCountTotal: Long, predictionCountSuccess: Long,
                           predictionCountFailed: Long, modelName: String, modelVersion: Long,
                           accumuPredictTimesMs: Long = 0, countDistribution0: Long, countDistribution1: Long,
                           countDistribution2: Long, countDistribution3: Long)
  extends Metric(metricName, 0, modelName, modelVersion) {
  var _predictionCountTotal: Long = predictionCountTotal
  var _predictionCountSuccess: Long = predictionCountSuccess
  var _predictionCountFailed: Long = predictionCountFailed
  var _accumuPredictTimesMs: Long = accumuPredictTimesMs
  var _countDistribution0: Long = countDistribution0
  var _countDistribution1: Long = countDistribution1
  var _countDistribution2: Long = countDistribution2
  var _countDistribution3: Long = countDistribution3

  override def debugString: String ={
    "metric_name=\"" + _metricName + "\", " + "model_name=\"" + _modelName + "\", " + "model_version=" + _modelVersion + ", " +
      "prediction_count_total=" + _predictionCountTotal + ", " + "prediction_count_success=" + _predictionCountSuccess +
    ", " + "prediction_count_failed=" + _predictionCountFailed + ", " + "total_predict_time_ms=" + _accumuPredictTimesMs + ",\n" +
    "count_distribution0=" + _countDistribution0 + ", count_distribution1=" + _countDistribution1 + ", count_distribution2=" +
    _countDistribution2 + ", count_distribution3=" + _countDistribution3
  }
}
