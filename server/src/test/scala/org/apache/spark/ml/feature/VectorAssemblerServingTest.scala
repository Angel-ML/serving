package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors, Vector}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object VectorAssemblerServingTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val dataset = spark.createDataFrame(
      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    val output = assembler.transform(dataset)
    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    output.select("features", "clicked").show(false)

    val res = trans(assembler)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(3).toString)
    res.printSchema()
  }

  def trans(model: VectorAssembler): SDFrame = {
    val transModel = ModelUtils.transTransformer(model).asInstanceOf[VectorAssemblerServing]
    val rowsFeatures = new Array[SRow](1)
    val training = Seq(Array(18, 1.0, Vectors.dense(0.0, 10.0, 0.5)))
    val featuresType = training(0).map{ feature =>
      feature match {
        case _ : Double => DoubleType
        case _ : String => StringType
        case _ : Integer => IntegerType
        case _ : Vector => new VectorUDT
      }
    }
    for (i <- 0 until rowsFeatures.length) {
      rowsFeatures(i) = new SRow(training(i))
    }

    var schema: StructType = null
    val iter = model.getInputCols.zip(featuresType).iterator
    while (iter.hasNext) {
      val (colName, featureType) = iter.next()
      if (schema == null) {
        schema = new StructType().add(new StructField(colName, featureType, true))
      } else {
        schema = schema.add(new StructField(colName, featureType, true))
      }
    }

    val dataset = new SDFrame(rowsFeatures)(schema)
    transModel.transform(dataset)
  }
}
