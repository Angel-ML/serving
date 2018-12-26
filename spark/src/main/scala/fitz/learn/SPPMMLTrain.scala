package fitz.learn

import java.io.{FileOutputStream, OutputStream}

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jpmml.model.JAXBUtil
import org.jpmml.sparkml.PMMLBuilder
import javax.xml.transform.stream.StreamResult

object SPPMMLTrain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .appName("JPMMLExample")
      .getOrCreate()

    val irisData: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/Iris.csv")

    val schema = irisData.schema

    val formula = new RFormula().setFormula("species ~ .")

    val classifier = new DecisionTreeClassifier()
      .setLabelCol(formula.getLabelCol)
      .setFeaturesCol(formula.getFeaturesCol)

    val pipeline = new Pipeline()
    pipeline.setStages(Array[PipelineStage](formula, classifier))

    val pipelineModel = pipeline.fit(irisData)

    // pipelineModel.write.overwrite().save("models/dtiris")

    val pmml = new PMMLBuilder(schema, pipelineModel).build()
    //JAXBUtil.marshalPMML(pmml, new StreamResult(System.out))
    val outputStream = new FileOutputStream("models/dtiris/DecisionTree.pmml")
    JAXBUtil.marshalPMML(pmml, new StreamResult(outputStream))
  }
}
