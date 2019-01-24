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
GET http://host:port/angelServing/v1.0//models/${MODEL_NAME}[/versions/${MODEL_VERSION}]
```  

其中```/versions/${MODEL_VERSION}```是可选的，如果没有指定，则会返回所有的版本

##### Examples #####

请求：

```
curl localhost:8501/angelServing/v1.0/models/lr
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

返回预测指标，包括成功次数、失败次数、平均预测时间等

##### 请求URL #####

```
GET http://host:port/angelServing/v1.0/monitoring/metrics
```

##### Examples #####

请求：

```
GET http://host:port/angelServing/v1.0/monitoring/metrics
```  

返回：

```
{"lr6_success" : metric_name="PredictSummary", prediction_count=5, model_name="lr", model_version=6, is_success=true, average_predict_time_ms=0.2}
```

### Predict API ###

用于预测服务的请求api，返回response.proto定义的数据的json表示

##### 请求URL #####

```
POST http://host:port/angelServing/v1.0/models/${MODEL_NAME}[/versions/${MODEL_VERSION}]:predict
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

##### Response format #####

Response proto的json字符串

##### Examples #####

请求：

```
curl -H "Content-Type: application/json" -X POST -d '{"instances": [{"x1":6.2, "x2":2.2, "x3":1.1, "x4":1.1}]}' localhost:8501/angelServing/v1.0/models/lr/versions/6:predict
```

返回：

```
[dType: DT_STRING
flag: IF_STRINGKEY_VECTOR
mv {
  s2s_map {
    key: "probability(1)"
    value: "0.10978009160848745"
  }
  s2s_map {
    key: "probability(-1)"
    value: "0.8902199083915125"
  }
  s2s_map {
    key: "y"
    value: "-1"
  }
}
]
```