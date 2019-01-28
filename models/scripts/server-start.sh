#!/usr/bin/env bash
# set SERVING_HOME and modify model_config_file's model base path(use absolute path)

export SERVING_HOME=

${SERVING_HOME}/bin/serving-submit \
  --port 8500 \
  --rest_api_port 8501 \
  --model_config_file ${SERVING_HOME}/models/angel/lr/config/model_config_file \
  --enable_metric_summary true