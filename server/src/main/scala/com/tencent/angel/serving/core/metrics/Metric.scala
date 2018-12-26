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

class PredictMetricSummary(metricName: String, predictionCount: Long = 0, averagePredictTimesMs: Long = 0,
                           modelName: String, modelVersion: Long, isSucess: Boolean = false,
                           summaryPeriod: Int = 30, accumuPredictTimesMs: Long = 0)
  extends Metric(metricName, 0, modelName, modelVersion) {
  var _predictionCount: Long = predictionCount
  var _summaryPeriod: Int = summaryPeriod
  var _averagePredictTimeMs: Long = averagePredictTimesMs
  var _isSucess: Boolean = isSucess
  var _accumuPredictTimesMs: Long = accumuPredictTimesMs

  override def debugString: String ={
    "metric_name=\"" + _metricName + "\", " + "prediction_count=" + _predictionCount + ", " +
    "model_name=\"" + _modelName + "\", " + "model_version=" + _modelVersion + ", " + "is_success=" +
    _isSucess + ", " + "average_predict_time_ms=" + _averagePredictTimeMs + ", " + "summary_period=" +
    _summaryPeriod
  }
}
