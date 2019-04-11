package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object CountVectorizerServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel.transform(df).show(false)

    val res = trans(cvm)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: CountVectorizerModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[CountVectorizerServingModel]
    val rowsFeatures = new Array[SRow](2)
    val training = Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )
    for (i <- 0 until rowsFeatures.length) {
      rowsFeatures(i) = new SRow(Array(training(i)._2))
    }

    val schema = new StructType().add(new StructField(model.getInputCol, ArrayType(StringType), true))
    val dataset = new SDFrame(rowsFeatures)(schema)
    transModel.transform(dataset)
  }
}
