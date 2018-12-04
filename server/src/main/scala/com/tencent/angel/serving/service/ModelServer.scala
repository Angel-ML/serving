package com.tencent.angel.serving.service

import io.grpc.ServerBuilder
import java.io.{FileInputStream, IOException}

import com.google.protobuf.StringValue
import org.slf4j.{Logger, LoggerFactory}
import com.tencent.angel.config.{Entry, Resource, ResourceAllocation}
import com.tencent.angel.config.ModelServerConfigProtos.{ModelConfig, ModelConfigList, ModelServerConfig}
import com.tencent.angel.config.PlatformConfigProtos.PlatformConfigMap
import com.tencent.angel.serving.core.{EventBus, ServableState, ServableStateMonitor, ServerCore}
import com.tencent.angel.serving.servables.angel.AngelPredictor
import com.tencent.angel.serving.serving.{BatchingParameters, ModelServerConfig}
import org.eclipse.jetty.servlet.ServletContextHandler
import com.sun.jersey.spi.container.servlet.ServletContainer
import com.tencent.angel.config.MonitoringConfigProtos.MonitoringConfig
import com.tencent.angel.servable.SessionBundleConfigProtos.{BatchingParameters, SessionBundleConfig}
import org.eclipse.jetty.servlet.ServletContextHandler.NO_SESSIONS


class ModelServer {

  import ModelServer.{readModelConfigFile, readPlatformConfigFile,
    defaultResourceAllocation, readBatchingParametersFile, readMonitoringConfigFile}

  private val LOG: Logger = LoggerFactory.getLogger(classOf[ModelServer])

  var serverCore: ServerCore = _
  var predictionServiceImpl: PredictionServiceImpl = _
  var modelServiceImpl: ModelServiceImpl = _
  var grpcServer: io.grpc.Server = _
  var httpServer: org.eclipse.jetty.server.Server = _

  def buildSingleModelConfig(modelName : String, modelBasePath: String): ModelServerConfig ={
    LOG.info("Building single Angel model file config: " +
      "model_name: " + modelName + " model_base_path: " + modelBasePath)
    val singelModel: ModelConfig = ModelConfig.newBuilder().setName(modelName)
      .setBasePath(modelBasePath).setModelPlatform("Angel").build()
    val modelServerConfig = ModelServerConfig.newBuilder().setModelConfigList(ModelConfigList.newBuilder().addConfig(singelModel)).build()
    modelServerConfig
  }

  def buildAndStart(serverOptions: Options): Unit = {
    val useSavedModel: Boolean = true

    if(serverOptions.grpc_port == 0) {
      LOG.info("serverOptions.grpc_port is not set.")
      return
    }

    if(serverOptions.model_base_path.isEmpty && serverOptions.model_config_file.isEmpty) {
      LOG.info("Both serverOptions.model_base_path and serverOptions.model_config_file are empty!")
      return
    }

    var modelServerConfig: ModelServerConfig = null
    if(serverOptions.model_config_file.isEmpty){
      modelServerConfig = buildSingleModelConfig(serverOptions.model_name,
        serverOptions.model_base_path)
    } else {
      modelServerConfig = readModelConfigFile(serverOptions.model_config_file)
    }

    var platformConfigMap: PlatformConfigMap = null
    if(serverOptions.platform_config_file.isEmpty) {
      val sessionBundleConfigBuilder = SessionBundleConfig.newBuilder()
      if(serverOptions.enable_batching) {
        val batchingParametersOrBuilder = BatchingParameters.newBuilder()
        if(serverOptions.batching_parameters_file.isEmpty) {
          batchingParametersOrBuilder.setThreadPoolName(StringValue.newBuilder().setValue("model_server_batch_threads"))
          sessionBundleConfigBuilder.setBatchingParameters(batchingParametersOrBuilder.build())
        } else {
          sessionBundleConfigBuilder.setBatchingParameters(readBatchingParametersFile(serverOptions.batching_parameters_file))
        }
      } else if(!serverOptions.platform_config_file.isEmpty) {
        LOG.info("serverOptions.batching_parameters_file is set without setting " +
          "serverOptions.enable_batching to true.")
      }
      val tags = serverOptions.saved_model_tags.split(",")
      for(tag <- tags) {
        sessionBundleConfigBuilder.addSavedModelTags(tag)
      }
      platformConfigMap = PlatformConfigUtil
        .createAngelPlatformConfigMap(sessionBundleConfigBuilder.build(), useSavedModel)
    } else {
      platformConfigMap = readPlatformConfigFile(serverOptions.platform_config_file)
    }

    val eventBus = new EventBus[ServableState]()
    val monitor = new ServableStateMonitor(eventBus, 1000)

    //val modelServerConfig: ModelServerConfig = readModelConfigFile(serverOptions.model_config_file)
    //val platformConfigMap: PlatformConfigMap = readPlatformConfigFile(serverOptions.platform_config_file)
    val totalResources: ResourceAllocation = defaultResourceAllocation()
    val servingContext: ServingContext = new ServingContext(eventBus, monitor, totalResources, platformConfigMap)

    servingContext.maxNumLoadRetries = serverOptions.max_num_load_retries
    servingContext.loadRetryIntervalMicros = serverOptions.load_retry_interval_micros
    servingContext.fileSystemPollWaitSeconds = serverOptions.file_system_poll_wait_seconds
    servingContext.flushFilesystemCaches = serverOptions.flush_filesystem_caches

    serverCore = new ServerCore(servingContext)
    serverCore.reloadConfig(modelServerConfig)

    predictionServiceImpl = new PredictionServiceImpl(serverCore, new AngelPredictor())
    modelServiceImpl = new ModelServiceImpl(serverCore)
    val serverBuilder: ServerBuilder[_ <: ServerBuilder[_]] = ServerBuilder.forPort(serverOptions.grpc_port)
    serverBuilder.addService(predictionServiceImpl)
    serverBuilder.addService(modelServiceImpl)
    grpcServer = serverBuilder.build()
    if(grpcServer == null) {
      LOG.info("Failed to BuildAndStart gRPC server.")
      return
    }
    grpcServerStart()
    LOG.info("Running gRPC ModelServer at " + serverOptions.grpc_port)

    if(serverOptions.http_port != 0) {
      if(serverOptions.http_port != serverOptions.grpc_port) {
        var monitoringConfig: MonitoringConfig = null
        if(!serverOptions.monitoring_config_file.isEmpty) {
          monitoringConfig = readMonitoringConfigFile(serverOptions.monitoring_config_file)
        }
        import org.eclipse.jetty.server.Server
        httpServer = new Server(serverOptions.http_port)
        val servletContextHandler = new ServletContextHandler(NO_SESSIONS)
        servletContextHandler.setContextPath("/")
        httpServer.setHandler(servletContextHandler)
        val servletHolder = servletContextHandler.addServlet(classOf[ServletContainer], "/*")
        servletHolder.setInitOrder(0)
        servletHolder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig")
        servletHolder.setInitParameter("com.sun.jersey.config.property.packages", "com.tencent.angel.serving.service.jersey")
        servletHolder.setInitParameter("jersey.config.server.provider.packages", "com.tencent.angel.serving.service.jersey.resources")
        // 自动将对象映射成json返回
        servletHolder.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature", "true")
        if(httpServer != null) {
          try {
            httpServer.start()
            LOG.info("Exporting HTTP/REST API at: " + serverOptions.http_port)
            httpServer.join()
          }
          catch {
            case ex: Exception => LOG.info("Error occurred while starting Jetty")
          }
        } else {
          LOG.info("Failed to start HTTP Server at " + serverOptions.http_port)
        }
      } else {
        LOG.info("server_options.http_port cannot be same as grpc_port. " +
          "Please use a different port for HTTP/REST API. " +
          "Skipped exporting HTTP/REST API.")
      }
    }

  }

  def waitForTermination(): Unit = {
    blockUntilShutdown()
  }

  /** Start serving requests. */
  @throws[IOException]
  private def grpcServerStart(): Unit = {
    grpcServer.start
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = { // Use stderr here since the logger may has been reset by its JVM shutdown hook.
        LOG.info("*** shutting down gRPC server since JVM is shutting down")
        ModelServer.this.stop()
        LOG.info("*** server shut down")
      }
    })
  }

  /** Stop serving requests and shutdown resources. */
  private def stop(): Unit = {
    if (grpcServer != null) grpcServer.shutdown
  }

  /**
    * Await termination on the main thread since the grpc library uses daemon threads.
    */
  @throws[InterruptedException]
  private def blockUntilShutdown(): Unit = {
    if (grpcServer != null) grpcServer.awaitTermination()
    if(httpServer != null) httpServer.wait()
  }
}

object ModelServer {

  var server: ModelServer = _

  @throws[IOException]
  def readPlatformConfigFile(platformConfigFile: String): PlatformConfigMap = {
    val is = new FileInputStream(platformConfigFile)
    PlatformConfigMap.parseFrom(is)
  }

  @throws[IOException]
  def readModelConfigFile(modelConfigFile: String): ModelServerConfig = {
    val is = new FileInputStream(modelConfigFile)
    ModelServerConfig.parseFrom(is)
  }

  @throws[IOException]
  def readBatchingParametersFile(batchingParametersFile: String): BatchingParameters = {
    val is = new FileInputStream(batchingParametersFile)
    BatchingParameters.parseFrom(is)
  }

  @throws[IOException]
  def readMonitoringConfigFile(monitoringConfigFile: String): MonitoringConfig = {
    val is = new FileInputStream(monitoringConfigFile)
    MonitoringConfig.parseFrom(is)
  }

  def defaultResourceAllocation(): ResourceAllocation = {
    val run = Runtime.getRuntime
    val available = (run.totalMemory() * 0.8).toLong
    ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), available)))
  }

  def getServerCore: ServerCore = {
    server.serverCore
  }
}

