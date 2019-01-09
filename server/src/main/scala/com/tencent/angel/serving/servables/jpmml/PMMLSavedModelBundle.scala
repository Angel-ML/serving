package com.tencent.angel.serving.servables.jpmml

import java.io.{File, FileInputStream, IOException}
import java.util

import com.tencent.angel.config.{Entry, Resource, ResourceAllocation}
import com.tencent.angel.core.saver.MetaGraphProtos._
import com.tencent.angel.utils.{InstanceUtils, ProtoUtils}
import com.tencent.angel.serving.apis.common.InstanceProtos.InstanceFlag
import com.tencent.angel.serving.apis.prediction.RequestProtos.Request
import com.tencent.angel.serving.apis.prediction.ResponseProtos.Response
import com.tencent.angel.serving.core.StoragePath
import com.tencent.angel.serving.servables.common.{RunOptions, Session, SavedModelBundle}
import javax.xml.bind.JAXBException
import org.dmg.pmml.{FieldName, PMML}
import org.jpmml.evaluator._
import org.jpmml.model.PMMLUtil
import org.jpmml.model.visitors.MemoryMeasurer
import org.slf4j.{Logger, LoggerFactory}
import org.xml.sax.SAXException
import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import scala.collection.JavaConversions._


class PMMLSavedModelBundle(val pmml: PMML) extends SavedModelBundle {
  override val session: Session = null
  override val metaGraphDef: MetaGraphDef = null

  private val evaluator: Evaluator = ModelEvaluatorFactory.newInstance.newModelEvaluator(pmml)
  evaluator.verify()

  private val inputFields: util.List[InputField] = evaluator.getInputFields
  private val fieldNameSet = new util.HashSet[String]()
  for (fieldName: FieldName <- EvaluatorUtil.getNames(inputFields)) {
    fieldNameSet.add(fieldName.getValue)
  }

  private val groupFields: util.List[InputField] = evaluator match {
    case groupFiled: HasGroupFields => groupFiled.getGroupFields
    case _ => new util.ArrayList[InputField]()
  }
  private val groupFieldNameSet = new util.HashSet[String]()
  for (fieldName: FieldName <- EvaluatorUtil.getNames(groupFields)) {
    groupFieldNameSet.add(fieldName.getValue)
  }

  private val targetFields: util.List[TargetField] = evaluator.getTargetFields
  private val outputFields: util.List[OutputField] = evaluator.getOutputFields
  private val resultFields: util.List[FieldName] = EvaluatorUtil.getNames(
    Lists.newArrayList(Iterables.concat(targetFields, outputFields))
  )

  override def runClassify(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = {
    runPredict(runOptions, request, responseBuilder)
  }

  override def runMultiInference(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = {
    runPredict(runOptions, request, responseBuilder)
  }

  override def runPredict(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = {
    val numInstances = request.getInstancesCount

    responseBuilder.setModelSpec(request.getModelSpec)
    val esb = new StringBuilder()
    (0 until numInstances).foreach { idx =>
      val instance = request.getInstances(idx)
      assert(instance.getFlag == InstanceFlag.IF_STRINGKEY_VECTOR)

      val dataMap = InstanceUtils.getStringKeyMap(instance)

      val missingInputFields = Sets.difference(fieldNameSet, dataMap.keySet())
      val missingGroupFields = Sets.difference(groupFieldNameSet, dataMap.keySet())

      val arguments = new util.LinkedHashMap[FieldName, FieldValue]()
      val outputRecords: util.Map[String, String] = new util.HashMap[String, String]()
      if (!missingInputFields.isEmpty) {
        esb.append(s"Instance ${instance.getName} Missing input field(s): " + missingInputFields).append("\n")
      } else if (!missingGroupFields.isEmpty) {
        esb.append(s"Instance ${instance.getName} Missing group field(s): " + missingGroupFields).append("\n")
      } else {
        try {
          for (inputField: InputField <- inputFields) {
            val inputFieldName = inputField.getName
            val rawValue = dataMap.get(inputFieldName.getValue)
            val inputFieldValue = inputField.prepare(rawValue)
            arguments.put(inputFieldName, inputFieldValue)
          }

          val newArguments = evaluator match {
            case hasGroupFields: HasGroupFields =>
              EvaluatorUtil.groupRows(hasGroupFields, Lists.newArrayList(arguments)).get(0)
            case _ => arguments
          }

          val results: util.Map[FieldName, _] = evaluator.evaluate(newArguments)
          for (fieldName: FieldName <- resultFields) {
            val name = fieldName.getValue
            val valueObject = EvaluatorUtil.decode(results.get(fieldName))
            val value = if (valueObject == null) {
              "N/A"
            } else {
              valueObject.toString
            }

            outputRecords.put(name, value)
          }

          responseBuilder.addPredictions(ProtoUtils.getInstance(instance.getName, outputRecords))
        } catch {
          case e: Exception => esb.append(e.getMessage).append("\n")
        }
      }

      outputRecords.clear()
      arguments.clear()
    }

    if (esb.nonEmpty) {
      responseBuilder.setError(esb.toString())
    }
  }

  override def runRegress(runOptions: RunOptions, request: Request, responseBuilder: Response.Builder): Unit = {
    runPredict(runOptions, request, responseBuilder)
  }

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
