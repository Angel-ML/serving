package org.apache.spark.ml.classification

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object OneVsRestServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    // load data file.
    val inputData = spark.read.format("libsvm")
      .load("data/sample_multiclass_classification_data.txt")

    // generate the train/test split.
    val Array(train, test) = inputData.randomSplit(Array(0.8, 0.2))

    // instantiate the base classifier
    val classifier = new LogisticRegression()
      .setMaxIter(10)
      .setTol(1E-6)
      .setFitIntercept(true)

    // instantiate the One Vs Rest Classifier.
    val ovr = new OneVsRest().setClassifier(classifier)

    // train the multiclass model.
    val ovrModel = ovr.fit(train)

    // score the model on test data.
    val predictions = ovrModel.transform(test)

    println(predictions.show())

    // obtain evaluator.
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    // compute the classification error on test data.
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1 - accuracy}")

    val res = trans(ovrModel)
    println(res.schema, res.columns.length, res.columns(0),"\n",
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString,
      res.getRow(0).get(2).toString, res.getRow(0).get(3).toString,
      res.getRow(0).get(4).toString)
    res.printSchema()
  }

  def trans(model: OneVsRestModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[OneVsRestServingModel]
    val rows = new Array[SRow](1)
    val size = 4
    val index = Array[Int](0, 1, 2, 3)
    val value = Array[Double](-0.222222, 0.5, -0.762712, -0.833333)
    for (i <- 0 until rows.length) {
      rows(i) = new SRow(Array(Vectors.dense(value)))
    }

    val schema = new StructType().add(new StructField(model.getFeaturesCol, new VectorUDT, true))
    val dataset = new SDFrame(rows)(schema)
    transModel.transform(dataset)
  }
}
