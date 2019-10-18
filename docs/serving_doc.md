# Angel Serving User Manual

## 1. Serving Server

## 1.1 Main Class
Main class is the entry point to start the serving server, in which you can configure the args required to run serving:

```
val parser = new OptionParser[Options]("ModelServer") {

      opt[Int]("port")
        .text("Port to listen on for gRPC API")
        .required()
        .action((x, c) => c.copy(grpc_port = x))
      opt[Int]("rest_api_port")
        .text("Port to listen on for HTTP/REST API.If set to zero " +
          "HTTP/REST API will not be exported. This port must be " +
          "different than the one specified in --port.")
        .required()
        .action((x, c) => c.copy(http_port = x))
      opt[Int]("rest_api_timeout_in_ms")
        .text("Timeout for HTTP/REST API calls.")
        .action((x, c) => c.copy(http_timeout_in_ms = x))
      opt[Boolean]("enable_batching")
        .text("enable batching.")
        .action((x, c) => c.copy(enable_batching = x))
      opt[String]("batching_parameters_file")
        .text("If non-empty, read an ascii BatchingParameters " +
          "protobuf from the supplied file name and use the " +
          "contained values instead of the defaults.")
        .action((x, c) => c.copy(batching_parameters_file = x))
      opt[String]("model_config_file")
        .text("If non-empty, read an ascii ModelServerConfig " +
          "protobuf from the supplied file name, and serve the " +
          "models in that file. This config file can be used to " +
          "specify multiple models to serve and other advanced " +
          "parameters including non-default version policy. (If " +
          "used, --model_name, --model_base_path are ignored.)" +
          "add the prefix `file:///` to model_base_path when use localfs")
        .action((x, c) => c.copy(model_config_file = x))
      opt[String]("hadoop_home")
        .text("hadoop_home " +
          "which contains hdfs-site.xml and core-site.xml.")
        .action((x, c) => c.copy(hadoop_home = x))
      opt[String]("model_name")
        .text("name of model (ignored " +
          "if --model_config_file flag is set")
        .action((x, c) => c.copy(model_name = x))
      opt[String]("model_base_path")
        .text("path to export (ignored if --model_config_file flag " +
          "is set, otherwise required), " +
          "add the prefix `file:///` to model_base_path when use localfs")
        .action((x, c) => c.copy(model_base_path = x))
      opt[String]("model_platform")
        .text("platform for model serving (ignored if --model_config_file flag " +
          "is set, otherwise required), ")
        .action((x, c) => c.copy(model_platform = x))
      opt[String]("saved_model_tags")
        .text("Comma-separated set of tags corresponding to the meta " +
          "graph def to load from SavedModel.")
        .action((x, c) => c.copy(saved_model_tags = x))
      opt[Int]("max_num_load_retries")
        .text("maximum number of times it retries loading a model " +
          "after the first failure, before giving up. " +
          "If set to 0, a load is attempted only once. " +
          "Default: 5")
        .action((x, c) => c.copy(max_num_load_retries = x))
      opt[Long]("load_retry_interval_micros")
        .text("The interval, in microseconds, between each servable " +
          "load retry. If set negative, it doesn't wait. " +
          "Default: 1 minute")
        .action((x, c) => c.copy(load_retry_interval_micros = x))
      opt[Int]("file_system_poll_wait_seconds")
        .text("interval in seconds between each poll of the file " +
          "system for new model version")
        .action((x, c) => c.copy(file_system_poll_wait_seconds = x))
      opt[Boolean]("flush_filesystem_caches")
        .text("If true (the default), filesystem caches will be " +
          "flushed after the initial load of all servables, and " +
          "after each subsequent individual servable reload (if " +
          "the number of load threads is 1). This reduces memory " +
          "consumption of the model server, at the potential cost " +
          "of cache misses if model files are accessed after " +
          "servables are loaded.")
        .action((x, c) => c.copy(flush_filesystem_caches = x))
      opt[Boolean]("enable_model_warmup")
        .text("Enables model warmup, which triggers lazy " +
          "initializations (such as TF optimizations) at load " +
          "time, to reduce first request latency.")
        .action((x, c) => c.copy(enable_model_warmup = x))
      opt[String]("monitoring_config_file")
        .text("If non-empty, read an ascii MonitoringConfig protobuf from " +
          "the supplied file name")
        .action((x, c) => c.copy(monitoring_config_file = x))
      opt[String]("metric_implementation")
        .text("Defines the implementation of the metrics to be used (logger, " +
          "syslog ...). ")
        .action((x, c) => c.copy(target_publishing_metric = x))
      opt[Boolean]("enable_metric_summary")
        .text("Enable summary for metrics, launch an async task.")
        .action((x, c) => c.copy(enable_metric_summary = x))
      opt[String]("count_distribution_bucket")
        .text("response time interval distribution.")
        .action((x, c) => c.copy(count_distribution_bucket = x))
      opt[String]("hadoop_job_ugi")
        .text("hadoop job ugi to access hdfs, Separated by commas, example:\"test, test\".")
        .action((x, c) => c.copy(hadoop_job_ugi = x))
      opt[String]("principal")
        .text("principal for kerberos auth.")
        .action((x, c) => c.copy(principal = x))
      opt[String]("keytab")
        .text("keytab for kerberos auth.")
        .action((x, c) => c.copy(keytab = x))
      opt[Int]("metric_summary_wait_seconds")
        .text("Interval in seconds between each summary of metrics." +
          "(Ignored if --enable_metric_summary=false)")
        .action((x, c) => c.copy(metric_summary_wait_seconds = x))
    }
```
`note:`Args with the required attribute must be specified

## 1.2 Model configuration

### Configuring the model by Args
* model_name
* model_base_path：model base path stores different version model named by version number
* model_platform: angel、pmml or torch, default: angel

### Configuring the model through a configuration file
* Model_config_file：Model_config_list can contain multiple config, each config corresponds to a model, set the specific parameters of the model in config, where name, base_path can not be empty。

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

* model_version_policy has three options: Latest、All and Specific

	```
	Latest：{
	      num_versions: 2 // number of model versions
	     }//Provide the latest two versions for service
	All：{}//Provide all versions for service
	Specific：{
	      versions: 3
	      versions: 1
			…
	     }//Provide the version specified in the list for service
	```

* examples(Three models serve simultaneously)

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

### notes
* Different versions under the model base path should be named numerically
* In the case configuring the model by Args, only a single model can be serviced, while through a configuration file can serve multiple models.

## 1.3 examples for start serving server
### example 1

```$xslt
   $SERVING_HOME/bin/serving-submit \
      --port 8500 \
      --rest_api_port 8501 \
      --model_base_path /path/to/model \
      --model_name lr \ 
      --model_platform angel \
      --enable_metric_summary true
   ```

### example 2
```$xslt
   $SERVING_HOME/bin/serving-submit \
      --port 8500 \
      --rest_api_port 8501 \
      --model_config_file /path/to/model_config_file \
      --enable_metric_summary true
   ```
