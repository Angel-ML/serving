package com.tencent.angel.serving.service

import io.grpc.ServerBuilder
import java.io.{FileInputStream, IOException}
import java.util.logging.Logger

import com.tencent.angel.config.{Entry, Resource, ResourceAllocation}
import com.tencent.angel.config.ModelServerConfigProtos.ModelServerConfig
import com.tencent.angel.config.PlatformConfigProtos.PlatformConfigMap
import com.tencent.angel.config.ResourceProtos.ResourceAllocation
import com.tencent.angel.serving.core.{EventBus, ServableState, ServableStateMonitor, ServerCore}
import com.tencent.angel.serving.servables.angel.AngelPredictor
import com.tencent.angel.serving.serving.ModelServerConfig
import org.eclipse.jetty.servlet.ServletContextHandler
import com.sun.jersey.spi.container.servlet.ServletContainer
import org.eclipse.jetty.servlet.ServletContextHandler.NO_SESSIONS
import scopt.OptionParser

case class Options() {
  var grpcPort: Int = 8500

  var httpPort: Int = 0
  var httpTimeoutInMs = 30000

  var enableBatching: Boolean = false
  var batchingParametersFile: String = _
  var model_name: String = _
  var maxNumLoadRetries: Int = 5
  var loadRetryIntervalMicros: Long = 1L * 60 * 1000 * 1000
  var fileSystemPollWaitSeconds: Int = 1
  var flushFilesystemCaches: Boolean = true
  var modelBasePath: String = _
  var savedModelTags: String = _
  var platformConfigFile: String = _
  var modelConfigFile: String = _
  var enableModelWarmup: Boolean = true
  var monitoringConfigFile: String = _
}

class ModelServer {

  import ModelServer.{readModelConfigFile, readPlatformConfigFile, defaultResourceAllocation}

  private val logger = Logger.getLogger(classOf[ModelServer].getName)

  var serverCore: ServerCore = _
  var predictionServiceImpl: PredictionServiceImpl = _
  var modelServiceImpl: ModelServiceImpl = _
  var grpcServer: io.grpc.Server = _
  var httpServer: org.eclipse.jetty.server.Server = _

  def buildAndStart(options: Options): Unit = {
    val eventBus = new EventBus[ServableState]()
    val monitor = new ServableStateMonitor(eventBus, 1000)
    val modelServerConfig: ModelServerConfig = readModelConfigFile(options.modelConfigFile)
    val platformConfigMap: PlatformConfigMap = readPlatformConfigFile(options.platformConfigFile)
    val totalResources: ResourceAllocation = defaultResourceAllocation()
    val servingContext: ServingContext = new ServingContext(eventBus, monitor, totalResources, platformConfigMap)

    servingContext.maxNumLoadRetries = options.maxNumLoadRetries
    servingContext.loadRetryIntervalMicros = options.loadRetryIntervalMicros

    serverCore = new ServerCore(servingContext)
    serverCore.reloadConfig(modelServerConfig)

    predictionServiceImpl = new PredictionServiceImpl(serverCore, new AngelPredictor())
    modelServiceImpl = new ModelServiceImpl(serverCore)
    val serverBuilder: ServerBuilder[_ <: ServerBuilder[_]] = ServerBuilder.forPort(options.grpcPort)
    serverBuilder.addService(predictionServiceImpl)
    serverBuilder.addService(modelServiceImpl)
    grpcServer = serverBuilder.build()
    import org.eclipse.jetty.server.Server
    httpServer = new Server(options.httpPort)
    val servletContextHandler = new ServletContextHandler(NO_SESSIONS)
    servletContextHandler.setContextPath("/")
    httpServer.setHandler(servletContextHandler)
    val servletHolder = servletContextHandler.addServlet(classOf[ServletContainer], "/*")
    servletHolder.setInitOrder(0)
    servletHolder.setInitParameter("jersey.config.server.provider.packages", "com.tencent.angel.serving.service.jersey.resources")
    // 自动将对象映射成json返回
    servletHolder.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature", "true")
    start()
  }

  def waitForTermination(): Unit = {
    blockUntilShutdown()
  }

  /** Start serving requests. */
  @throws[IOException]
  private def start(): Unit = {
    grpcServer.start
    logger.info("Server started, listening on " + grpcServer.getPort)
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = { // Use stderr here since the logger may has been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down")
        ModelServer.this.stop()
        System.err.println("*** server shut down")
      }
    })
    try {
      httpServer.start()
      httpServer.join()
    }
    catch {
      case ex: Exception => System.out.print("Error occurred while starting Jetty")
    }
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
  /**
    * Main method.  This comment makes the linter happy.
    */
  var server: ModelServer = _

  case class Params(
                     port: Int = 8500,
                     rest_api_port: Int = 0,
                     rest_api_timeout_in_ms: Int = 0,
                     enable_batching: Boolean = true,
                     batching_parameters_file: String = "",
                     model_config_file: String = "",
                     model_name: String = "",
                     model_base_path: String = "",
                     max_num_load_retries: Int = 0,
                     load_retry_interval_micros: Long = 0,
                     file_system_poll_wait_seconds: Int = 0,
                     flush_filesystem_caches: Boolean = true,
                     enable_model_warmup: Boolean = true,
                     monitoring_config_file: String = ""
                   ) extends AbstractParams[Params]

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("ModelServer") {

      opt[Int]("port")
        .text("Port to listen on for gRPC API")
        .required()
        .action((x, c) => c.copy(port = x))
      opt[Int]("rest_api_port")
        .text("Port to listen on for HTTP/REST API.")
        .required()
        .action((x, c) => c.copy(rest_api_port = x))
      opt[Int]("rest_api_timeout_in_ms")
        .text("Timeout for HTTP/REST API calls.")
        .action((x, c) => c.copy(rest_api_timeout_in_ms = x))
      opt[Boolean]("enable_batching")
        .text("enable batching.")
        .action((x, c) => c.copy(enable_batching = x))
      opt[String]("batching_parameters_file")
        .text("")
        .action((x, c) => c.copy(batching_parameters_file = x))
      opt[String]("model_config_file")
        .text("")
        .action((x, c) => c.copy(model_config_file = x))
      opt[String]("model_name")
        .text("")
        .action((x, c) => c.copy(model_name = x))
      opt[String]("model_base_path")
        .text("")
        .action((x, c) => c.copy(model_base_path = x))
      opt[Int]("max_num_load_retries")
        .text("")
        .action((x, c) => c.copy(max_num_load_retries = x))
      opt[Long]("load_retry_interval_micros")
        .text("")
        .action((x, c) => c.copy(load_retry_interval_micros = x))
      opt[Int]("file_system_poll_wait_seconds")
        .text("")
        .action((x, c) => c.copy(file_system_poll_wait_seconds = x))
      opt[Boolean]("flush_filesystem_caches")
        .text("")
        .action((x, c) => c.copy(flush_filesystem_caches = x))
      opt[Boolean]("enable_model_warmup")
        .text("")
        .action((x, c) => c.copy(enable_model_warmup = x))
      opt[String]("monitoring_config_file")
        .text("")
        .action((x, c) => c.copy(monitoring_config_file = x))
    }
    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params): Unit ={
    val options = Options()
    options.grpcPort = params.port
    options.httpPort = params.rest_api_port
    options.httpTimeoutInMs = params.rest_api_timeout_in_ms
    options.enableBatching = params.enable_batching
    options.batchingParametersFile = params.batching_parameters_file
    options.modelConfigFile = params.model_config_file
    options.monitoringConfigFile = params.monitoring_config_file
    options.model_name = params.model_name
    options.modelBasePath = params.model_base_path
    options.maxNumLoadRetries = params.max_num_load_retries
    options.loadRetryIntervalMicros = params.load_retry_interval_micros
    options.fileSystemPollWaitSeconds = params.file_system_poll_wait_seconds
    options.flushFilesystemCaches = params.flush_filesystem_caches
    options.enableModelWarmup = params.enable_model_warmup
    server = new ModelServer
    server.buildAndStart(options)
    server.waitForTermination()
  }

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

  def defaultResourceAllocation(): ResourceAllocation = {
    val run = Runtime.getRuntime
    val available = (run.totalMemory() * 0.8).toLong
    ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), available)))
  }

  def getServerCore: ServerCore = {
    server.serverCore
  }
}

