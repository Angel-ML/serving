package com.tencent.angel.serving.service

import io.grpc.Server
import io.grpc.ServerBuilder
import java.io.{FileInputStream, IOException}
import java.util.logging.Logger

import com.tencent.angel.confg.{Entry, Resource, ResourceAllocation}
import com.tencent.angel.config.ModelServerConfigProtos.ModelServerConfig
import com.tencent.angel.config.PlatformConfigProtos.PlatformConfigMap
import com.tencent.angel.serving.core.{EventBus, ServableState, ServableStateMonitor, ServerCore}
import com.tencent.angel.serving.servables.angel.AngelPredictor
import com.tencent.angel.serving.serving.ModelServerConfig

case class Options() {
  var grpcPort: Int = 8500

  var httpPort: Int = 0
  var httpTimeoutInMs = 30000

  var enableBatching: Boolean = false
  var batchingParametersFile: String = _
  var model_name: String = _
  var maxNumLoadRetries: Int = 5
  var LoadRetryIntervalMicros: Long = 1L * 60 * 1000 * 1000
  var fileSystemPollWaitSeconds: Int = 1
  var flushFilesystemCaches: Boolean = true
  var modelBasePath: String = _
  var savedModelTags: String = _
  var platformConfigFile: String = _
  var modelConfigFile: String = _
  var enableModelWarmup: Boolean = true
}

class ModelServer(val serverBuilder: ServerBuilder[_ <: ServerBuilder[_]], val port: Int) {

  import ModelServer.{readModelConfigFile, readPlatformConfigFile, defaultResourceAllocation}

  private val logger = Logger.getLogger(classOf[ModelServer].getName)

  var server: Server = _

  def this(port: Int) {
    this(ServerBuilder.forPort(port), port)
  }

  def buildAndStart(options: Options): Unit = {
    val eventBus = new EventBus[ServableState]()
    val monitor = new ServableStateMonitor(eventBus, 1000)
    val modelServerConfig: ModelServerConfig = readModelConfigFile(options.modelConfigFile)
    val platformConfigMap: PlatformConfigMap = readPlatformConfigFile(options.platformConfigFile)
    val totalResources: ResourceAllocation = defaultResourceAllocation()
    val servingContext: ServingContext = new ServingContext(eventBus, monitor, totalResources, platformConfigMap)

    servingContext.maxNumLoadRetries = options.maxNumLoadRetries
    servingContext.loadRetryIntervalMicros = options.LoadRetryIntervalMicros

    val serverCore: ServerCore = new ServerCore(servingContext)
    serverCore.reloadConfig(modelServerConfig)

    val predictionServiceImpl: PredictionServiceImpl = new PredictionServiceImpl(serverCore, new AngelPredictor())
    server = serverBuilder.addService(predictionServiceImpl).build()
    start()
  }

  def waitForTermination(): Unit = {
    blockUntilShutdown()
  }

  /** Start serving requests. */
  @throws[IOException]
  private def start(): Unit = {
    server.start
    logger.info("Server started, listening on " + port)
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = { // Use stderr here since the logger may has been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down")
        ModelServer.this.stop()
        System.err.println("*** server shut down")
      }
    })
  }

  /** Stop serving requests and shutdown resources. */
  private def stop(): Unit = {
    if (server != null) server.shutdown
  }

  /**
    * Await termination on the main thread since the grpc library uses daemon threads.
    */
  @throws[InterruptedException]
  private def blockUntilShutdown(): Unit = {
    if (server != null) server.awaitTermination()
  }
}

object ModelServer {
  /**
    * Main method.  This comment makes the linter happy.
    */

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val options = Options()
    val server = new ModelServer(options.grpcPort)
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

}

