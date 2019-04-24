package org.apache.spark.ml.clustering

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object LocalLDAServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    // Loads data.
    val dataset = spark.read.format("libsvm")
      .load("data/sample_lda_libsvm_data.txt")

    // Trains a LDA model.
    val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    // Shows the result.
    val transformed = model.transform(dataset)
    transformed.show(false)

    val res = trans(model)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: LDAModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[LocalLDAServingModel]
    val rows = new Array[SRow](1)
    val size = 11
    val index = Array[Int](0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val value = Array[Double](4, 1, 0, 0, 4, 5, 1, 3, 0, 1, 0)
    for (i <- 0 until rows.length) {
      rows(i) = new SRow(Array(Vectors.sparse(size, index, value)))
    }

//    val schema = new StructType().add(new StructField(model.getFeaturesCol, new VectorUDT, true))
    val dataset = transModel.prepareData(rows)
    transModel.transform(dataset)
  }
}
