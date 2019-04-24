package org.apache.spark.ml.feature

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object IDFServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    println("tokenizer: \n", wordsData.show())

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors
    println("hashingTF: \n", featurizedData.show())

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show()

    val pipe = new Pipeline().setStages(Array(tokenizer, hashingTF, idf))
    val model = pipe.fit(sentenceData)

    val res = trans(model)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: PipelineModel): SDFrame = {
    val transModel = ModelUtils.transModel(model)
    val rowsFeatures = new Array[SRow](1)
    val training = Seq(
      (0.0, "Hi I heard about Spark")
    )
    for (i <- 0 until rowsFeatures.length) {
      rowsFeatures(i) = new SRow(Array(training(i)._2))
    }

//    val schema = new StructType().add(new StructField("sentence", StringType, true))
    val dataset = transModel.prepareData(rowsFeatures)
    transModel.transform(dataset)
  }

//  def trans(model: IDFModel): SDFrame = {
//    val transModel = ModelUtils.transModel(model).asInstanceOf[IDFServingModel]
//    val rowsFeatures = new Array[SRow](1)
//    val training = Seq(
//      (0.0, "Hi I heard about Spark")
//    )
//    val size = 20
//    val index = Array[Int](0,5,9,17)
//    val value = Array[Double](1,3,5,7)
//    for (i <- 0 until rowsFeatures.length) {
//      rowsFeatures(i) = new SRow(Array(Vectors.sparse(size, index, value)))
//    }
//
//    val schema = new StructType().add(new StructField("rawFeatures", new VectorUDT, true))
//    val dataset = new SDFrame(rowsFeatures)(schema)
//    transModel.transform(dataset)
//  }
}
