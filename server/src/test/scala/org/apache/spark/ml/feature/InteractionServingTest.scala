package org.apache.spark.ml.feature

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object InteractionServingTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val df = spark.createDataFrame(Seq(
      (1, 1, 2, 3, 8, 4, 5),
      (2, 4, 3, 8, 7, 9, 8),
      (3, 6, 1, 9, 2, 3, 6),
      (4, 10, 8, 6, 9, 4, 5),
      (5, 9, 2, 7, 10, 7, 3),
      (6, 1, 1, 4, 2, 8, 4)
    )).toDF("id1", "id2", "id3", "id4", "id5", "id6", "id7")

    println(df.show())
    val assembler1 = new VectorAssembler().
      setInputCols(Array("id2", "id3", "id4")).
      setOutputCol("vec1")

    val assembled1 = assembler1.transform(df)

    val assembler2 = new VectorAssembler().
      setInputCols(Array("id5", "id6", "id7")).
      setOutputCol("vec2")

    val assembled2 = assembler2.transform(assembled1).select("id1", "vec1", "vec2")

    val interaction = new Interaction()
      .setInputCols(Array("id1", "vec1", "vec2"))
      .setOutputCol("interactedCol")

    val interacted = interaction.transform(assembled2)

    interacted.show(truncate = false)

    val res = trans(interaction)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(3).toString)
    res.printSchema()
  }

  def trans(model: Interaction): SDFrame = {
    val transModel = ModelUtils.transTransformer(model).asInstanceOf[InteractionServing]
    val rowsFeatures = new Array[SRow](1)
    val index = Array(0,1,2)
    val training = Seq(
      Array(1, Vectors.sparse(3, index, Array(1.0,2.0,3.0)), Vectors.sparse(3, index, Array(8.0,4.0,5.0))))
    for (i <- 0 until rowsFeatures.length) {
      rowsFeatures(i) = new SRow(training(i))
    }

    val schema = new StructType().add(new StructField("id1", IntegerType, true))
      .add(new StructField("vec1", new VectorUDT, true))
      .add(new StructField("vec2", new VectorUDT, true))
    val dataset = new SDFrame(rowsFeatures)(schema)
    transModel.transform(dataset)
  }
}
