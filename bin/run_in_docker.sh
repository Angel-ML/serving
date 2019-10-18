#!/bin/bash
set -e

$SERVING_HOME/bin/serving-submit \
  --port $PORT \
  --rest_api_port $REST_API_PORT \
  --model_base_path $MODEL_BASE_PATH \
  --model_name $MODEL_NAME \
  --model_platform $MODEL_PLATFORM \
  --enable_metric_summary $ENABLE_METRIC_SUMMARY
