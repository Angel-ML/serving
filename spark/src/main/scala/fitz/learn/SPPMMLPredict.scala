package fitz.learn


import scala.collection.mutable
import java.io.{BufferedReader, FileInputStream, FileReader, IOException}
import java.util

import com.google.common.collect.{Iterables, Lists, Sets}
import javax.xml.bind.JAXBException
import org.dmg.pmml.{DataType, FieldName, PMML}
import org.jpmml.evaluator._
import org.jpmml.evaluator.tree._
import org.jpmml.model.PMMLUtil


object SPPMMLPredict {

  def getEvaluator(fileName: String = "models/dtiris/DecisionTree.pmml"): Evaluator = {
    var inputStream: FileInputStream = null
    var evaluator: Evaluator = null

    try {
      inputStream = new FileInputStream(fileName)
      val pmml: PMML = PMMLUtil.unmarshal(inputStream)
      val modelEvaluatorFactory: ModelEvaluatorFactory = ModelEvaluatorFactory.newInstance
      evaluator = modelEvaluatorFactory.newModelEvaluator(pmml)
      evaluator.verify()
    } catch {
      case e1@(_: IOException | _: JAXBException) => e1.printStackTrace()
    } finally {
      if (inputStream != null) {
        inputStream.close()
      }
    }

    evaluator
  }

  def evaluate(line: String)(implicit evaluator: Evaluator, head: Map[String, Int]): util.Map[FieldName, _] = {
    val iter = evaluator.getInputFields.iterator()
    val target = evaluator.getTargetFields.get(0).getName.getValue
    val arguments = new util.HashMap[FieldName, FieldValue]()
    val items = line.trim.split(",")
    while (iter.hasNext) {
      val field: InputField = iter.next()
      val fieldName = field.getName

      if (fieldName.getValue != target) {
        val idx = head(fieldName.getValue)
        arguments.put(fieldName, field.prepare(items(idx)))
      }
    }

    evaluator.evaluate(arguments)
  }

  def getTarget(res: util.Map[FieldName, _])(implicit evaluator: Evaluator): Unit = {
    val targetField = evaluator.getTargetFields.get(0).getName
    val outputFields = evaluator.getOutputFields.asScala

    println(EvaluatorUtil.decode(res.get(targetField)))

  }


  def main(args: Array[String]): Unit = {
    implicit val evaluator: Evaluator = getEvaluator()

    val reader = new BufferedReader(new FileReader("data/Iris.csv"))
    val head = reader.readLine().trim.split(",")
    implicit val headMap: Map[String, Int] = head.indices.map { idx => (head(idx), idx) }.toMap

    var line: String = reader.readLine()
    while (line != null && line.nonEmpty) {
      val result = evaluate(line) // LinkedHashMap
      getTarget(result)
      line = reader.readLine()
    }

    val inputFields = evaluator.getInputFields
    val targetFields = evaluator.getTargetFields
    val outputFields = evaluator.getOutputFields
    //val resultFields: List[_ <: ResultField] = Lists.newArrayList(Iterables.concat(targetFields, outputFields))

    // BatchUtil.formatRecords(outputRecords, EvaluatorUtil.getNames(resultFields), createCellFormatter(this.missingValues.size() > 0 ? this.missingValues.get(0) : null))

    println(inputFields)
    println(targetFields)
    println(outputFields)
  }
}
