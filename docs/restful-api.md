## RESTful API ##

The request and response is a JSON object, In case of error, all APIs will return a JSON object in the response body with error 
as key and the error message as the value::

```
{
  "error": <error message string>
}
```

### Model status API ###

It returns the status of a model in the ModelServer, if successfully returns a json representation defined in GetModelStatusResponse protobuf.

##### URL #####

```
GET http://host:port/v1/models/${MODEL_NAME}[/versions/${MODEL_VERSION}]
```  

```/versions/${MODEL_VERSION}```is optional. If omitted status for all versions is returned in the response.

##### Examples #####

request：

```
curl localhost:8501/v1/models/lr
```  

response：

```
{
  "model_version_status": [{
    "version": "6",
    "state": "AVAILABLE"
  }]
}
```

### Prediction Metrics API ###

Returns the summary information of the predicted indicators, including the total number of accumulated requests, 
the total number of accumulated request successes, the total number of accumulated request failures, the total 
time taken for the cumulative request to succeed, Specifies the number of successful request accumulations in 
the distribution interval. The default interval of the distribution interval is 5ms, which is 0-5ms, 5-10ms, and 10-15ms respectively.
And the number of request successes in the 15+ms interval, you can set the distribution interval, such as --count_distribution_bucket="1,5,8".

##### URL #####

```
GET http://host:port/monitoring/prometheus/metrics
```

##### Examples #####

request：

```
curl http://host:port/monitoring/prometheus/metrics
```  

response：

```
{
  "models": {
    "lrplus": {
      "versions": {
        "6": {
          "model_name": "lrplus",
          "model_version": 6,
          "prediction_count_total": "5",
          "prediction_count_success": "5",
          "total_predict_time_ms": 1.0,
          "count_distribution0": "5"
        }
      }
    },
    "lr": {
      "versions": {
        "5": {
          "model_name": "lr",
          "model_version": 5,
          "prediction_count_total": "10",
          "prediction_count_success": "10",
          "total_predict_time_ms": 6.0,
          "count_distribution0": "10"
        },
        "6": {
          "model_name": "lr",
          "model_version": 6,
          "prediction_count_total": "10",
          "prediction_count_success": "8",
          "prediction_count_failed": "2",
          "total_predict_time_ms": 89.0,
          "count_distribution0": "7",
          "count_distribution3": "1"
        }
      }
    }
  }

```

### Predict API ###

predict service api

##### URL #####

```
POST http://host:port/v1/models/${MODEL_NAME}[/versions/${MODEL_VERSION}]:predict
```  

```/versions/${MODEL_VERSION}```is optional. If omitted the latest version is used.

##### Request format #####

The data is placed in the list whose key is instances：

```
{"instances": [{"values": [1, 2, 3, 4], "key": 1}]}
```

```{
  "instances": [
    [0.0, 1.1, 2.2],
    [3.3, 4.4, 5.5],
    ...
  ]
}
```

PMML request data format is kv map:

```
{"instances": [{"values": {"x1":6.2, "x2":2.2, "x3":1.1, "x4":1.}, "key": 1}]}
```

```
{
  "instances": [
    {"values": {"x1":6.2, "x2":2.2, "x3":1.1, "x4":1.3}, "key": 1},
    {"values": {"x1":3.2, "x2":4.2, "x3":2.1, "x4":1.5}, "key": 2},
    ...
  ]
}
```

```
{
  "instances": [
    {"x1":6.2, "x2":2.2, "x3":1.1, "x4":1.3},
    {"x1":3.2, "x2":4.2, "x3":2.1, "x4":1.5},
    ...
  ]
}
```

Angel serving's restful api also supports sparse input. The sparse index value of this format needs to be placed in the list whose key is sparseIndices.
The value needs to be placed in the list whose key is sparseValues. The name cannot be omitted.  

```
{
  "instances": [
     {
       "sparseIndices":[97,3,4,41,109,16,115,53,117,23,119,27,61],
       "sparseValues":[0.64,0.36,0.41,0.42,0.20,0.26,0.67,0.11,0.23,0.39,0.16,0.45,0.68]
     },
     ...
  ]
}
```

```note```：It can get the schema of the predictive model (such as feature name, data type, feature dimension, etc.) through the model's status restful api.
##### Examples #####
request：

```
curl localhost:8501/v1/models/lr
```

response：

Angel Platform
```
{
  "model_version_status": [{
    "version": "6",
    "state": "AVAILABLE"
  }],
  "typeMap": {
    "valueType": "DT_FLOAT",
    "keyType": "DT_INT32"
  },
  "dim": "123"
}
```
PMML
```$xslt
{
  "model_version_status": [{
    "version": "6",
    "state": "AVAILABLE"
  }],
  "typeMap": {
    "x1": "DT_DOUBLE",
    "x2": "DT_DOUBLE",
    "x3": "DT_DOUBLE",
    "x4": "DT_DOUBLE"
  }
}
```

##### Response format #####

Response return a json object

```
﻿{
  "predictions": [
    {
      object
    }
  ]
}
```

In case of error, will return a JSON object in the response body with error

```$xslt
{
  "error": [error message]
}
```

##### Examples #####

request：

```
curl -H "Content-Type: application/json" -X POST -d '{"instances": [{"x1":6.2, "x2":2.2, "x3":1.1, "x4":1.1}]}' localhost:8501/v1/models/lr/versions/6:predict
```

response：

```
{
  "predictions": [
    {
      "probability(1)":"0.07969969495447403",
      "probability(-1)":"0.920300305045526",
      "y":"-1"
    }
  ]
}
```