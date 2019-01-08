package com.tencent.angel.serving.servables.jpmml

import java.io.{File, FileInputStream, IOException}

import com.tencent.angel.config.{Entry, Resource, ResourceAllocation}
import com.tencent.angel.core.saver.MetaGraphProtos
import com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse
import com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse
import com.tencent.angel.serving.apis.prediction.{ClassificationProtos, InferenceProtos, PredictProtos, RegressionProtos}
import com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse
import com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse
import com.tencent.angel.serving.core.StoragePath
import com.tencent.angel.serving.servables.angel
import com.tencent.angel.serving.servables.common.SavedModelBundle
import javax.xml.bind.JAXBException
import org.dmg.pmml.PMML
import org.jpmml.evaluator.{Evaluator, ModelEvaluatorFactory}
import org.jpmml.model.PMMLUtil
import org.jpmml.model.visitors.MemoryMeasurer
import org.slf4j.{Logger, LoggerFactory}
import org.xml.sax.SAXException

class PMMLSavedModelBundle(val pmml: PMML) extends SavedModelBundle {
  override val session: angel.Session = null
  override val metaGraphDef: MetaGraphProtos.MetaGraphDef = null
  private val evaluator: Evaluator = ModelEvaluatorFactory.newInstance.newModelEvaluator(pmml)

  override def runClassify(runOptions: angel.RunOptions, request: ClassificationProtos.ClassificationRequest, responseBuilder: ClassificationResponse.Builder): Unit = ???

  override def runMultiInference(runOptions: angel.RunOptions, request: InferenceProtos.MultiInferenceRequest, responseBuilder: MultiInferenceResponse.Builder): Unit = ???

  override def runPredict(runOptions: angel.RunOptions, request: PredictProtos.PredictRequest, responseBuilder: PredictResponse.Builder): Unit = {

  }

  override def runRegress(runOptions: angel.RunOptions, request: RegressionProtos.RegressionRequest, responseBuilder: RegressionResponse.Builder): Unit = ???

  override def unLoad(): Unit = {
    PMMLSavedModelBundle.unLoad()
  }
}


object PMMLSavedModelBundle {
  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private var pmml: PMML = _

  def create(path: StoragePath): PMMLSavedModelBundle = {
    var inputStream: FileInputStream = null
    try {
      LOG.info("Begin to load model ...")
      inputStream = new FileInputStream(path)
      pmml = PMMLUtil.unmarshal(inputStream)
      LOG.info("End to load model ...")
      new PMMLSavedModelBundle(pmml)
    } catch {
      case e: JAXBException =>
        e.printStackTrace()
        null.asInstanceOf[PMMLSavedModelBundle]
      case e: SAXException =>
        e.printStackTrace()
        null.asInstanceOf[PMMLSavedModelBundle]
      case e: IOException =>
        e.printStackTrace()
        null.asInstanceOf[PMMLSavedModelBundle]
    } finally {
      if (inputStream != null) {
        inputStream.close()
      }
    }
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
    if (pmml != null) {
      val memoryMeasurer = new MemoryMeasurer
      memoryMeasurer.applyTo(pmml)
      ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), memoryMeasurer.getSize)))
    } else {
      ResourceAllocation(List(Entry(Resource("CPU", 0, "Memmory"), 0L)))
    }
  }

  def unLoad(): Unit = {
    pmml = null
  }
}
