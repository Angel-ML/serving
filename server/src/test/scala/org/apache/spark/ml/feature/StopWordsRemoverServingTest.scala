package org.apache.spark.ml.feature

import java.util

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object StopWordsRemoverServingTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show(false)

    val res = trans(remover)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: StopWordsRemover): SDFrame = {
    val transModel = ModelUtils.transTransformer(model).asInstanceOf[StopWordsRemoverServing]
    val rowsFeatures = new Array[SRow](2)
    val training = Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )
//    for (i <- 0 until rowsFeatures.length) {
//      rowsFeatures(i) = new SRow(Array(training(i)._2))
//    }
    val data: util.Map[String, Seq[String]] = new util.HashMap[String, Seq[String]]
    data.put(model.getInputCol, Seq("I", "saw", "the", "red", "balloon"))

//    val schema = new StructType().add(new StructField(model.getInputCol, ArrayType(StringType), true))
    val dataset = transModel.prepareData(data)
    transModel.transform(dataset)
  }
}
