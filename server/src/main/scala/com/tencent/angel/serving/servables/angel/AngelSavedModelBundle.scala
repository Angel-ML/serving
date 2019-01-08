package com.tencent.angel.serving.servables.angel

import java.util
import java.io.File

import com.tencent.angel.config.{Entry, Resource, ResourceAllocation}
import com.tencent.angel.core.graph.TensorProtos
import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef
import com.tencent.angel.ml.core.PredictResult
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.data.LabeledData
import com.tencent.angel.ml.core.local.{LocalEvnContext, LocalModel}
import com.tencent.angel.ml.core.utils.JsonUtils
import com.tencent.angel.serving.apis.prediction.RequestProtos.Request
import com.tencent.angel.serving.apis.prediction.ResponseProtos.Response
import com.tencent.angel.serving.core.StoragePath
import com.tencent.angel.serving.servables.common.SavedModelBundle
import org.slf4j.{Logger, LoggerFactory}
import com.tencent.angel.utils.ProtoUtils
import com.tencent.angel.serving.servables.Utils.predictResult2TensorProto
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
    /*
    val modelSpec = request.getModelSpec
    val iter = request.getInputsMap.entrySet().iterator()

    responseBuilder.setModelSpec(modelSpec)

    while(iter.hasNext) {
      val entry = iter.next()
      val key = entry.getKey
      val value = entry.getValue

      val res: PredictResult = model.predict(new LabeledData(ProtoUtils.toVector(value), 0.0))
      LOG.info(s"res: ${res.getText}")
      responseBuilder.putOutputs(key, predictResult2TensorProto(res))
    }
    */
  }

  override def runRegress(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = {

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
