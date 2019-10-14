package org.apache.spark.ml.feature

import java.util

import org.apache.spark.ml.data.{SDFrame, SRow, UDF}
import org.apache.spark.ml.feature.RFormulaModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.types._

class RFormulaServingModel(stage: RFormulaModel) extends ServingModel[RFormulaServingModel] {

  private var value_Type: String = ""

  override def copy(extra: ParamMap): RFormulaServingModel = {
    new RFormulaServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    checkCanTransform(dataset.schema)
    transformLabel(ModelUtils.transModel(stage.pipelineModel).transform(dataset))
  }

  override def transformSchema(schema: StructType): StructType = {
    checkCanTransform(schema)
    val withFeatures = stage.pipelineModel.transformSchema(schema)
    if (stage.resolvedFormula.label.isEmpty || hasLabelCol(withFeatures)) {
      withFeatures
    } else if (schema.exists(_.name == stage.resolvedFormula.label)) {
      val nullable = schema(stage.resolvedFormula.label).dataType match {
        case _: NumericType | BooleanType => false
        case _ => true
      }
      StructType(withFeatures.fields :+ StructField(stage.getLabelCol, DoubleType, nullable))
    } else {
      // Ignore the label field. This is a hack so that this transformer can also work on test
      // datasets in a Pipeline.
      withFeatures
    }
  }

  override val uid: String = stage.uid

  private def transformLabel(dataset: SDFrame): SDFrame = {
    val labelName = stage.resolvedFormula.label
    if (labelName.isEmpty || hasLabelCol(dataset.schema)) {
      dataset
    } else if (dataset.schema.exists(_.name == labelName)) {
      dataset.schema(labelName).dataType match {
        case _: NumericType | BooleanType =>
          val labelUDF = UDF.make[Double, Double](label => label, false)
          dataset.withColum(labelUDF.apply(stage.getLabelCol, dataset(labelName)))
        case other =>
          throw new IllegalArgumentException("Unsupported type for label: " + other)
      }
    } else {
      // Ignore the label field. This is a hack so that this transformer can also work on test
      // datasets in a Pipeline.
      dataset
    }
  }

  private def checkCanTransform(schema: StructType) {
    val columnNames = schema.map(_.name)
    require(!columnNames.contains(stage.getFeaturesCol), "Features column already exists.")
    require(
      !columnNames.contains(stage.getLabelCol) || schema(stage.getLabelCol).dataType.isInstanceOf[NumericType],
      "Label column already exists and is not of type NumericType.")
  }

  protected def hasLabelCol(schema: StructType): Boolean = {
    schema.map(_.name).contains(stage.getLabelCol)
  }

  override def prepareData(rows: Array[SRow]): SDFrame = {
    var features = stage.resolvedFormula.terms.map(term => term(0))
    features = features :+ stage.resolvedFormula.label
    var schema: StructType = null
    val featuresTypes = rows(0).values.map{ feature =>
      feature match {
        case _ : Double => {
          setValueType("double")
          DoubleType
        }
        case _ : String =>
          setValueType("string")
          StringType
        case _ : Integer =>
          setValueType("int")
          IntegerType
        case _ : Vector =>
          setValueType("double")
          new VectorUDT
        case _ : Array[String] =>
          setValueType("string")
          ArrayType(StringType)
      }
    }
    if (features.length == featuresTypes.length - 1) {
      (0 until featuresTypes.length).foreach{ index =>
        if (index == 0) {
          schema = new StructType().add(new StructField("id", featuresTypes(index), true))
        } else {
          schema = schema.add(new StructField(features(index - 1), featuresTypes(index), true))
        }
      }
    } else if (features.length == featuresTypes.length) {
      val iter = features.zip(featuresTypes).iterator
      while (iter.hasNext) {
        val (colName, featureType) = iter.next()
        if (schema == null) {
          schema = new StructType().add(new StructField(colName, featureType, true))
        } else {
          schema = schema.add(new StructField(colName, featureType, true))
        }
      }
    } else {
      throw new Exception("the inputData is not match the struct of data in train model")
    }

    new SDFrame(rows)(schema)
  }

  override def prepareData(feature: util.Map[String, _]): SDFrame = {
    var featuresCol = stage.resolvedFormula.terms.map(term => term(0))
    featuresCol.foreach(println)
    var schema: StructType = null
    val rows = new Array[Any](feature.size())
    val featureNames = feature.keySet.toArray
    var index = 0
    featuresCol.foreach{ colName =>
      if (!feature.containsKey(colName)) {
        throw new Exception (s"the ${colName} is not included in the input col(s)")
      } else {
        val value = feature.get(colName)
        val valueType = value match {
          case _ : Double => {
            setValueType("double")
            DoubleType
          }
          case _ : String =>
            setValueType("string")
            StringType
          case _ : Integer =>
            setValueType("int")
            IntegerType
          case _ : Vector =>
            setValueType("double")
            new VectorUDT
          case _ : Array[String] =>
            setValueType("string")
            ArrayType(StringType)
        }
        if (schema == null) {
          schema = new StructType().add(new StructField(colName, valueType, true))
        } else {
          schema = schema.add(new StructField(colName, valueType, true))
        }
        rows(index) = value
        index += 1
      }
    }
    new SDFrame(Array(new SRow(rows)))(schema)
  }
}

object RFormulaServingModel {
  def apply(stage: RFormulaModel): RFormulaServingModel = new RFormulaServingModel(stage)
}