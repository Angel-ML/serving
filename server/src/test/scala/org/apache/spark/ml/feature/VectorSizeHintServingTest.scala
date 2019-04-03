package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

object VectorSizeHintServingTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val df7 = spark.createDataFrame(
      Seq(
        (0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0),
        (0, 18, 1.0, Vectors.dense(0.0, 10.0), 0.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour","mobile","userFeatures"))
      .setOutputCol("features")

    val output = assembler.transform(df7)
    //    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    //    output.select("features", "clicked").show(false)

    /**
      * VectorSizeHint
      * VectorSizeHint允许用户显式指定列的向量大小
      * 以便VectorAssembler或可能需要知道向量大小的其他变换器可以将该列用作输入
      */
    val sizeHint = new VectorSizeHint()
      .setInputCol("userFeatures")
      .setHandleInvalid("skip")
      .setSize(3)

    val datasetWithSize = sizeHint.transform(df7)
    //    println("Rows where 'userFeatures' is not the right size are filtered out")
    //    datasetWithSize.show(false)

    val outputHint = assembler.transform(datasetWithSize)
    println(outputHint.show())
    //    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    //    output.select("features", "clicked").show(false)


    val res = trans(sizeHint)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: VectorSizeHint): SDFrame = {
    val transModel = ModelUtils.transTransformer(model).asInstanceOf[VectorSizeHintServing]
    val rowsFeatures = new Array[SRow](2)
    val training = Seq(
      Array(18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0),
      Array(18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
    for (i <- 0 until rowsFeatures.length) {
      println(training(i))
      rowsFeatures(i) = new SRow(training(i))
    }

    val schema = new StructType().add(new StructField("hour", IntegerType, true))
    .add(new StructField("mobile", DoubleType, true))
    .add(new StructField("userFeatures", new VectorUDT, true))
    val dataset = new SDFrame(rowsFeatures)(schema)
    transModel.transform(dataset)
  }
}
