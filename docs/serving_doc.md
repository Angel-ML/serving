# Serving 使用文档

## 1. Serving Server

## 1.1 Main类
Main为启动serving server的入口，在其中可配置运行serving所需的参数，所有参数类型及默认值放置在Options类中，具体参数如下：

```
case class Options(
                   grpc_port: Int = 8500,//grpc端口号
                   http_port: Int = 0,//http端口号
                   http_timeout_in_ms: Int = 3000,//http服务超时等待时长（单位：毫秒）
                   enable_batching: Boolean = true,//是否启用批量
                   batching_parameters_file: String = "",//批量参数配置文件路径
                   model_config_file: String = "",//模型配置文件路径
                   platform_config_file: String = "",//平台配置文件路径
                   model_name: String = "default",//模型名称
                   model_base_path: String = "",//模型路径
                   model_platform: String = "angel",//模型运行平台
                   hadoop_home: String = "",//hadoop_home路径
                   saved_model_tags: String = "serve",//模型标签
                   max_num_load_retries: Int = 5,//最大加载重试次数
                   load_retry_interval_micros: Long = 60*1000*1000,//加载重试间隔（单位：微秒）
                   file_system_poll_wait_seconds: Int = 1,//文件系统轮训等待时间（单位：秒）
                   flush_filesystem_caches: Boolean = true,//是否刷新文件系统缓存
                   enable_model_warmup: Boolean = true,//模型是否启用热启动
                   monitoring_config_file: String = "",//监视配置文件路径
                   metric_summary_wait_seconds: Int = 30,//统计指标摘要间隔时间（单位：秒）
                   enable_metric_summary: Boolean = true,//是否启用指标摘要
                   target_publishing_metric: String = "logger"//以日志形式输出指标，还可以通过控制台输出"syslog"
                 ) extends AbstractOptions[Options]
```

其中并不是所有的参数都是必须的，以下列出运行serving所必须的参数：
* 配置服务方式： grpc端口号，http端口号，两个端口号需要不同 
* 选择一种配置模型的方式：配置模型名称和模型路径 或 模型配置文件路径

## 1.2 模型配置

### 配置参数
* 模型名称
* 模型路径：下存放了以数字命名的不同模型版本文件夹，版本文件夹中存放该版本模型的参数，构建图等。模型路径目前支持本地路径和HDFS路径。
* 模型配置文件路径：模型配置文件的全路径，文件中包含了模型的配置信息，如：名称，路径，平台，服务规则等。如果配置了‘模型配置文件路径’，模型名称和模型路径的配置将失效。

### 模型配置文件
* Model_config_list结构：Model_config_list中可包含多个config，每个config对应一个模型，在config中设置模型的具体参数，其中name，base_path不能为空。

	```
	model_config_list: {
	config: {
	  name: "",
	  base_path: "",
	  model_platform: "",
	  model_version_policy: {
	      …
	  }
	 }，
	config: {
	  …
	},
	… …
	}
	```

* model_version_policy有三种选择

	```
	Latest：{
	      num_versions: 2  //版本个数
	     }//提供最新的两个版本进行服务
	All：{}//提供所有版本进行服务
	Specific：{
	      versions: 3 //版本号
	      versions: 1
			…
	     }//提供列表中的版本进行服务
	```

* 模型配置文件例子(三个模型同时服务)

	```
	model_config_list: {
	config: {
	  name: "lr",
	  base_path: " file:///f:/model/lr",
	  model_platform: "Angel",
	  model_version_policy: {
	      latest: {
	      num_versions: 2
	     }
	  }
	 }，
	config: {
	  name: "linear",
	  base_path: " file:///f:/model/linear",
	  model_platform: "Angel",
	  model_version_policy: {
	     all: {}
	  }
	},
	config: {
	  name: "robust",
	  base_path: "file:///f:/model/robust",
	  model_platform: "Angel",
	  model_version_policy: {
	     specific: {
	      versions: 3
	      versions: 1
	     }
	  }
	 }
	}
	```

### 注意事项
* 本地文件路径需要在路径前加入 “file:///”，如file:///f:/
* 模型路径下的不同版本应以数字形式命名
* 只配置模型名称和模型路径的情况下，只能对单个模型进行服务。配置了‘模型配置文件路径’，可以对多个模型进行服务。

## 1.3 启动serving server的例子
在命令行中输入以下命令并运行Main函数便可启动服务，参数之间以空格分开。
### 例子一
grpc端口号：8500， http端口号：8501， 模型名称：lr， 模型路径：“file:///f:/model/lr”

```
--port 8500 --rest_api_port 8501 --model_name "lr" --model_base_path "file:///f:/model/lr"
```

### 例子二
grpc端口号：8500， http端口号：8501，模型配置文件路径：“file:///f:/model/model_config_file.txt”

```
--port 8500 --rest_api_port 8501 --model_config_file "file:///f:/model/model_config_file.txt"
```

# 2. Serving Client
* RPCClient：使用的端口与server端定义的grpc端口一致
* HTTPClient：使用的端口与server端定义的http端口一致