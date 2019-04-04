## RESTful API ##

Serving的request和response的数据都是json格式，即使预测错误也会返回错误信息的json对象:

```
{
  "error": <error message string>
}
```

### Model status API ###

返回请求的服务的模型的状态，如果请求成功返回定义在GetModelStatusResponse protobuf的json表示.

##### 请求URL #####

```
GET http://host:port/v1/models/${MODEL_NAME}[/versions/${MODEL_VERSION}]
```  

其中```/versions/${MODEL_VERSION}```是可选的，如果没有指定，则会返回所有的版本

##### Examples #####

请求：

```
curl localhost:8501/v1/models/lr
```  

返回：

```
{
  "model_version_status": [{
    "version": "6",
    "state": "AVAILABLE"
  }]
}
```

### Prediction Metrics API ###

返回预测指标的总结信息，包括累加请求总数、累加请求成功总数、累加请求失败总数、累加请求成功总耗时、      
指定分布区间内请求成功累加次数，分布区间默认间隔为5ms，分别有0-5ms、5-10ms、10-15ms   
以及15+ms区间内的请求成功次数，可以设置分布区间，比如--count_distribution_bucket="1,5,8"

##### 请求URL #####

```
GET http://host:port/monitoring/prometheus/metrics
```

##### Examples #####

请求：

```
curl http://host:port/monitoring/prometheus/metrics
```  

返回：

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

用于预测服务的请求api，返回的数据为json表示

##### 请求URL #####

```
POST http://host:port/v1/models/${MODEL_NAME}[/versions/${MODEL_VERSION}]:predict
```  

其中```/versions/${MODEL_VERSION}```是可选的，如果没有指定，则会使用最新版本做预测

##### Request format #####

数据放入key为instances的list中：

```
{"instances": [{"values": [1, 2, 3, 4], "key": 1}]}
```

可以省略命名：

```{
  "instances": [
    [0.0, 1.1, 2.2],
    [3.3, 4.4, 5.5],
    ...
  ]
}
```

Pmml的数据输入为map格式:

```
{"instances": [{"values": {"x1":6.2, "x2":2.2, "x3":1.1, "x4":1.}, "key": 1}]}
```

可以省略命名:

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

Angel serving的restful api还支持稀疏的输入数据，该格式的稀疏索引值需要放入key为sparseIndices的list  
值需要放入key为sparseValues的list中，命名不能省略  

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

```注```：如果用户不知道预测模型的scheme(如特征名称，数据类型，特征维度等)，可以通过获取模型的status restful api 
得到，然后根据其scheme填充预测json数据
##### Examples #####
请求：

```
curl localhost:8501/v1/models/lr
```

返回：

angel平台
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
pmml平台
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

Response 返回的结果为json对象

```
﻿{
  "predictions": [
    {
      object
    }
  ]
}
```

若预测错误则会返回

```$xslt
{
  "error": string
}
```

##### Examples #####

请求：

```
curl -H "Content-Type: application/json" -X POST -d '{"instances": [{"x1":6.2, "x2":2.2, "x3":1.1, "x4":1.1}]}' localhost:8501/angelServing/v1.0/models/lr/versions/6:predict
```

返回：

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