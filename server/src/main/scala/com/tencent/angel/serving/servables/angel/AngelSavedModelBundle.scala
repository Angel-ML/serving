package com.tencent.angel.serving.servables.angel

import java.util
import java.io.File

import com.tencent.angel.config.{Entry, Resource, ResourceAllocation}
import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef
import com.tencent.angel.ml.core.PredictResult
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.data.LabeledData
import com.tencent.angel.ml.core.local.data.LocalMemoryDataBlock
import com.tencent.angel.ml.core.local.{LocalEvnContext, LocalModel}
import com.tencent.angel.ml.core.utils.JsonUtils
import com.tencent.angel.serving.apis.common.TypesProtos
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse
import com.tencent.angel.serving.apis.prediction.RequestProtos.Request
import com.tencent.angel.serving.apis.prediction.ResponseProtos.Response
import com.tencent.angel.serving.core.StoragePath
import com.tencent.angel.serving.servables.common.{RunOptions, SavedModelBundle, Session}
import org.slf4j.{Logger, LoggerFactory}
import com.tencent.angel.utils.{InstanceUtils, ProtoUtils}
import org.ehcache.sizeof.SizeOf

class AngelSavedModelBundle(model: LocalModel) extends SavedModelBundle {
  private val LOG = LoggerFactory.getLogger(classOf[AngelSavedModelBundle])

  override val session: Session = null
  override val metaGraphDef: MetaGraphDef = null

  override def unLoad(): Unit = {
    AngelSavedModelBundle.unLoad()
  }

  override def runClassify(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = ???

  override def runMultiInference(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = ???

  override def runPredict(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = {
    val modelSpec = request.getModelSpec
    responseBuilder.setModelSpec(modelSpec)

    val numInst = request.getInstancesCount
    if (numInst == 1) {
      try {
        val instance = request.getInstances(0)
        val vector = InstanceUtils.getVector(instance)
        val predictResult: PredictResult = model.predict(new LabeledData(vector, 0.0, instance.getName))
        responseBuilder.addPredictions(ProtoUtils.getInstance(instance.getName, toMap(predictResult)))
      } catch {
        case e: Exception => responseBuilder.setError(e.getMessage)
      }
    } else {
      val esb = new StringBuilder

      try {
        val maxUseMemroy: Long = 100 * SizeOf.newInstance().deepSizeOf(request)
        val dataBlock = new LocalMemoryDataBlock(numInst, maxUseMemroy)
        (0 until numInst).foreach{ idx =>
          val instance = request.getInstances(idx)
          val vector = InstanceUtils.getVector(instance)
          dataBlock.put(new LabeledData(vector, 0.0, instance.getName))
        }
        val predictResults: List[PredictResult] = model.predict(dataBlock)
        predictResults.zipWithIndex.foreach{ case (predictResult, idx) =>
          try {
            val instance = request.getInstances(idx)
            if (predictResult == null) {
              throw new Exception(s"Error in ${instance.getName}, the predictResult is null.")
            }
            responseBuilder.addPredictions(ProtoUtils.getInstance(instance.getName, toMap(predictResult)))
          } catch {
            case e: Exception => esb.append(e.getMessage).append("\n")
          }
        }
      } catch {
        case e: Exception => esb.append(e.getMessage).append("\n")
      }

      responseBuilder.setError(esb.toString())
    }
  }

  override def runRegress(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = ???

  private def toMap(predictResult: PredictResult): util.HashMap[String, String] = {
    val tempMap = new util.HashMap[String, String]()

    tempMap.put("sid", predictResult.sid)
    tempMap.put("pred", predictResult.pred.toString)
    tempMap.put("proba", predictResult.proba.toString)
    tempMap.put("predLabel", predictResult.predLabel.toString)
    tempMap.put("trueLabel", predictResult.trueLabel.toString)
    tempMap.put("attached", predictResult.attached.toString)

    tempMap
  }

  override def fillInputInfo(responseBuilder: GetModelStatusResponse.Builder): Unit = {
    model.graph.valueType match {
      case "string" => responseBuilder.putTypeMap("valueType", TypesProtos.DataType.DT_STRING)
      case "int" => responseBuilder.putTypeMap("valueType", TypesProtos.DataType.DT_INT32)
      case "long" => responseBuilder.putTypeMap("valueType", TypesProtos.DataType.DT_INT64)
      case "float" => responseBuilder.putTypeMap("valueType", TypesProtos.DataType.DT_FLOAT)
      case "double" => responseBuilder.putTypeMap("valueType", TypesProtos.DataType.DT_DOUBLE)
      case _ => responseBuilder.putTypeMap("valueType", TypesProtos.DataType.DT_INVALID)
    }

    responseBuilder.setDim(model.graph.indexRange)
  }
}

object AngelSavedModelBundle {
  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private var model: LocalModel = _

  def create(path: StoragePath): SavedModelBundle = {
    // load
    val graphJsonFile = s"$path${File.separator}graph.json"
    LOG.info(s"the graph file is $graphJsonFile")

    assert(new File(graphJsonFile).exists())

    val conf = SharedConf.get()
    conf.set(MLConf.ML_JSON_CONF_FILE, graphJsonFile)
    conf.setJson()

    println(JsonUtils.J2Pretty(conf.getJson))

    LOG.info(s"model load path is $path ")

     // update model load path
     conf.set(MLConf.ML_LOAD_MODEL_PATH, path)

    val model = new LocalModel(conf)
    LOG.info(s"buildNetwork for model")
    model.buildNetwork()

    LOG.info(s"start to load parameters for model")
    model.loadModel(LocalEvnContext(), path)

    LOG.info(s"model has loaded!")

    new AngelSavedModelBundle(model)
  }

  def resourceEstimate(modelPath: String): ResourceAllocation = {
    if (modelPath != null) {
      val modelFile = new File(modelPath)
      if (modelFile.exists()) {
        val fileSize = (modelFile.getTotalSpace * 1.2).toLong
        ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), fileSize)))
      } else {
        ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), 0L)))
      }
    } else {
      ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), 0L)))
    }
  }

  def estimateResourceRequirement(modelPath: String): ResourceAllocation = {
    if (model != null) {
      val sizeOf =  SizeOf.newInstance()
      val size = sizeOf.deepSizeOf(model)
      ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), size)))
    } else {
      ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), 0L)))
    }
  }

  def unLoad(): Unit = {
    model = null
  }
}
