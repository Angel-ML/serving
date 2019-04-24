package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object HashingTFServingTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val wordsData = spark.createDataFrame(Seq(
      (0.0, Array("hi", "i", "heard", "about", "spark")),
      (0.0, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (1.0, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("label", "words")

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors
    println(featurizedData.show())

    val res = trans(hashingTF)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: HashingTF): SDFrame = {
    val transModel = ModelUtils.transTransformer(model).asInstanceOf[HashingTFServing]
    val rowsFeatures = new Array[SRow](3)
    val training = Seq(
      (0.0, Seq("hi", "i", "heard", "about", "spark")),
      (0.0, Seq("I", "wish", "Java", "could", "use", "case", "classes")),
      (1.0, Seq("Logistic", "regression", "models", "are", "neat"))
    )
    for (i <- 0 until rowsFeatures.length) {
      rowsFeatures(i) = new SRow(Array(training(i)._2))
    }

//    val schema = new StructType().add(new StructField("words", ArrayType(StringType), true))
    val dataset = transModel.prepareData(rowsFeatures)
    transModel.transform(dataset)
  }
}
