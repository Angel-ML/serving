package com.tencent.angel.serving.service

import io.grpc.ServerBuilder
import java.io.{FileInputStream, IOException}
import java.util.logging.Logger

import com.tencent.angel.config.{Entry, Resource, ResourceAllocation}
import com.tencent.angel.config.ModelServerConfigProtos.ModelServerConfig
import com.tencent.angel.config.PlatformConfigProtos.PlatformConfigMap
import com.tencent.angel.serving.core.{EventBus, ServableState, ServableStateMonitor, ServerCore}
import com.tencent.angel.serving.servables.angel.AngelPredictor
import com.tencent.angel.serving.serving.ModelServerConfig
import org.eclipse.jetty.servlet.ServletContextHandler
import com.sun.jersey.spi.container.servlet.ServletContainer
import org.eclipse.jetty.servlet.ServletContextHandler.NO_SESSIONS


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

  def defaultResourceAllocation(): ResourceAllocation = {
    val run = Runtime.getRuntime
    val available = (run.totalMemory() * 0.8).toLong
    ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), available)))
  }

  def getServerCore: ServerCore = {
    server.serverCore
  }
}

