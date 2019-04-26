package org.apache.spark.ml.classification

import java.util
import java.util.UUID

import org.apache.spark.ml.data._
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.transformer.ServingModel
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.types._

class OneVsRestServingModel(stage: OneVsRestModel)
  extends ServingModel[OneVsRestServingModel] {

  override def copy(extra: ParamMap): OneVsRestServingModel = {
    new OneVsRestServingModel(stage.copy(extra))
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema, true)

    val origCols = dataset.schema.map(f => SCol(f.name))

    // add an accumulator column to store predictions of all the models
    val accColName = "mbc$acc" + UUID.randomUUID().toString
    val initUDF = UDF.make[Map[Int, Double]](() => Map[Int, Double]())
    val newDataset = dataset.withColum(initUDF(accColName))
    var tmpColName = ""

    // update the accumulator column with the result of prediction of models
    val aggregatedDataset = stage.models.zipWithIndex.foldLeft[SDFrame](newDataset) {
      case (df, (model, index)) =>
        val rawPredictionCol = model.getRawPredictionCol
        val columns = origCols ++ List(SCol(rawPredictionCol), SCol(accColName))

        // add temporary column to store intermediate scores and update
        tmpColName = "mbc$tmp" + UUID.randomUUID().toString
        val updateUDF = UDF.make[Map[Int, Double], Map[Int, Double], Vector](
          (predictions: Map[Int, Double], prediction: Vector) =>
            predictions + ((index, prediction(1))), false
        )

        model.setFeaturesCol(stage.getFeaturesCol)
        val transformedDataset = ModelUtils.transModel(model).transform(newDataset).select(SCol())
        val updatedDataset = transformedDataset
          .withColum(updateUDF(tmpColName, SCol(accColName), SCol(rawPredictionCol)))
        val newColumns = origCols ++ List(SCol(tmpColName))

        // switch out the intermediate column with the accumulator column
        updatedDataset.select(SCol())//.withColumnRenamed(tmpColName, accColName)
    }

    // output the index of the classifier with highest confidence as prediction
    val labelUDF = UDF.make[Double, Map[Int, Double]](
      predictions => predictions.maxBy(_._2)._1.toDouble, false)

    // output label and label metadata as prediction
    aggregatedDataset
      .withColum(labelUDF(stage.getPredictionCol, SCol(tmpColName)).setSchema(stage.getPredictionCol, stage.labelMetadata))
      .drop(new SimpleCol(tmpColName))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemaImpl(schema, false, stage.getClassifier.featuresDataType)
  }

  /**
    * Validates and transforms the input schema with the provided param map.
    *
    * @param schema input schema
    * @param fitting whether this is in fitting
    * @param featuresDataType  SQL DataType for FeaturesType.
    *                          E.g., `VectorUDT` for vector features.
    * @return output schema
    */
  def validateAndTransformSchemaImpl(
                                            schema: StructType,
                                            fitting: Boolean,
                                            featuresDataType: DataType): StructType = {
    // TODO: Support casting Array[Double] and Array[Float] to Vector when FeaturesType = Vector
    SchemaUtils.checkColumnType(schema, stage.getFeaturesCol, featuresDataType)
    if (fitting) {
      SchemaUtils.checkNumericType(schema, stage.getLabelCol)

      this match {
        case p: HasWeightCol =>
          if (isDefined(p.weightCol) && p.getWeightCol.nonEmpty) {
            SchemaUtils.checkNumericType(schema, p.getWeightCol)
          }
        case _ =>
      }
    }
    SchemaUtils.appendColumn(schema, stage.getPredictionCol, DoubleType)
  }

  override val uid: String = stage.uid

  override def prepareData(rows: Array[SRow]): SDFrame = {
    if (stage.isDefined(stage.featuresCol)) {
      val schema = new StructType().add(new StructField(stage.getFeaturesCol, new VectorUDT, true))
      new SDFrame(rows)(schema)
    } else {
      throw new Exception (s"featuresCol of ${stage} is not defined!")
    }
  }

  override def prepareData(feature: util.Map[String, _]): SDFrame = {
    if (stage.isDefined(stage.featuresCol)) {
      val featureName = feature.keySet.toArray
      if (!featureName.contains(stage.getFeaturesCol)) {
        throw new Exception (s"the ${stage.getFeaturesCol} is not included in the input col(s)")
      } else if (!feature.get(stage.getFeaturesCol).isInstanceOf[Vector]) {
        throw new Exception (s"the type of col ${stage.getFeaturesCol} is not Vector")
      } else {
        val schema = new StructType().add(new StructField(stage.getFeaturesCol, new VectorUDT, true))
        val rows =  Array(new SRow(Array(feature.get(stage.getFeaturesCol))))
        new SDFrame(rows)(schema)
      }
    } else {
      throw new Exception (s"featuresCol of ${stage} is not defined!")
    }
  }
}

object OneVsRestServingModel {
  def apply(stage: OneVsRestModel): OneVsRestServingModel = new OneVsRestServingModel(stage)
}