package com.tencent.angel.serving.servables.spark

import java.util

import com.google.common.collect.Sets
import com.tencent.angel.config.{Entry, Resource, ResourceAllocation}
import com.tencent.angel.core.saver.MetaGraphProtos
import com.tencent.angel.ml.core.PredictResult
import com.tencent.angel.serving.apis.common.InstanceProtos.InstanceFlag
import com.tencent.angel.serving.apis.common.TypesProtos
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse
import com.tencent.angel.serving.apis.prediction.RequestProtos
import com.tencent.angel.serving.apis.prediction.ResponseProtos.Response
import com.tencent.angel.serving.core.StoragePath
import com.tencent.angel.serving.servables.common.{RunOptions, SavedModelBundle, Session}
import com.tencent.angel.serving.sources.SystemFileUtils
import com.tencent.angel.utils.{InstanceUtils, ProtoUtils}
import org.apache.spark.ml.data.SDFrame
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.utils.ModelUtils
import org.ehcache.sizeof.SizeOf

class SparkSavedModelBundle(servingModel: ServingModel[_]) extends SavedModelBundle {
  override val session: Session = null
  override val metaGraphDef: MetaGraphProtos.MetaGraphDef = null

  override def runClassify(runOptions: RunOptions, request: RequestProtos.Request, responseBuilder: Response.Builder): Unit = ???

  override def runMultiInference(runOptions: RunOptions, request: RequestProtos.Request, responseBuilder: Response.Builder): Unit = ???

  override def runPredict(runOptions: RunOptions, request: RequestProtos.Request, responseBuilder: Response.Builder): Unit = {
    val numInstances = request.getInstancesCount

    responseBuilder.setModelSpec(request.getModelSpec)
    val outputRecords: util.Map[String, SDFrame] = new util.HashMap[String, SDFrame]()
    val esb = new StringBuilder()
    (0 until numInstances).foreach { idx =>
      val instance = request.getInstances(idx)
      try {
        val dataMap = InstanceUtils.getStringKeyMap(instance)
        val result = servingModel.transform(servingModel.prepareData(dataMap))

        outputRecords.put("result", result)

        responseBuilder.addPredictions(ProtoUtils.getInstance(instance.getName, outputRecords))
      } catch {
        case e: Exception => esb.append(e.getMessage).append("\n")
      }
    }
    outputRecords.clear()

    if (esb.nonEmpty) {
      responseBuilder.setError(esb.toString())
    }
  }

  override def runRegress(runOptions: RunOptions, request: RequestProtos.Request, responseBuilder: Response.Builder): Unit = ???

  override def unLoad(): Unit = {
    SparkSavedModelBundle.unLoad()
  }

  override def fillInputInfo(responseBuilder: GetModelStatusResponse.Builder): Unit = ???

  override def getInputInfo(): (TypesProtos.DataType, TypesProtos.DataType, Long) = ???
}

object SparkSavedModelBundle {
  private var model: SparkSavedModelBundle = _
  private var spark: SparkSession = _
  private var servingModel: ServingModel[_] = _

  def create(path: StoragePath): SparkSavedModelBundle = {
    spark = SparkSession.builder()
      .appName(s"SparkServing_$path")
      .master("local")
      .getOrCreate()

    val metadata = ModelUtils.loadMetadata(path, spark)
    val model = ModelUtils.loadModel(metadata.className, path)
    val servingModel = ModelUtils.transModel(model)

    new SparkSavedModelBundle(servingModel)
  }

  def resourceEstimate(modelPath: String): ResourceAllocation = {
    if (modelPath != null) {
      if (SystemFileUtils.fileExist(modelPath)) {
        val fileSize = (SystemFileUtils.getTotalSpace(modelPath) * 1.2).toLong
        ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), fileSize)))
      } else {
        ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), 0L)))
      }
    } else {
      ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), 0L)))
    }
  }

  def estimateResourceRequirement(modelPath: String): ResourceAllocation = {
    if (servingModel != null) {
      val sizeOf =  SizeOf.newInstance()
      val size = sizeOf.deepSizeOf(servingModel)
      // LOG.info("pmml model size is: " + size)
      ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), size)))
    } else {
      ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), 0L)))
    }
  }

  def unLoad(): Unit = {
    model = null
    spark = null
    servingModel = null
  }

}
