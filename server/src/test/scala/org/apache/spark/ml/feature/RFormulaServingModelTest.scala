package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object RFormulaServingModelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val dataset = spark.createDataFrame(Seq(
      (7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")
    println(dataset.show())

    val formula = new RFormula()
      .setFormula("clicked ~ country + hour")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val output = formula.fit(dataset)
    val out = output.transform(dataset)
    out.show()
    out.select("features", "label").show()


    val res = trans(output)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(4).toString, res.getRow(0).get(5).toString)
    res.printSchema()
  }

  def trans(model: RFormulaModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[RFormulaServingModel]
    val rowsFeatures = new Array[SRow](3)
    val training = Seq(
      Array("US", 18, 1.0),
      Array("CA", 12, 0.0),
      Array("NZ", 15, 0.0)
    )
    for (i <- 0 until rowsFeatures.length) {
      rowsFeatures(i) = new SRow(training(i))
    }

    val schema = new StructType()
      .add(new StructField("country", StringType, true))
      .add(new StructField("hour", IntegerType, true))
      .add(new StructField("clicked", DoubleType, true))
    val dataset = new SDFrame(rowsFeatures)(schema)
    transModel.transform(dataset)
  }
}
