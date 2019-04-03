//package org.apache.spark.ml.feature
//
//import org.apache.spark.ml.data.{SDFrame, SRow}
//import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
//import org.apache.spark.ml.feature.utils.ModelUtils
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
//
//object VectorAssemblerServingTest {
//  def main(args: Array[String]): Unit = {
//    val spark: SparkSession = SparkSession.builder()
//      .appName("MLTest")
//      .master("local")
//      .getOrCreate()
//
//    val dataset = spark.createDataFrame(
//      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
//    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")
//
//    val assembler = new VectorAssembler()
//      .setInputCols(Array("hour", "mobile", "userFeatures"))
//      .setOutputCol("features")
//
//    val output = assembler.transform(dataset)
//    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
//    output.select("features", "clicked").show(false)
//
//    val res = trans(assembler)
//    println(res.schema, res.columns.length, res.columns(0),
//      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
//    res.printSchema()
//  }
//
//  def trans(model: VectorAssembler): SDFrame = {
//    val transModel = ModelUtils.transTransformer(model).asInstanceOf[VectorAssemblerServing]
//    val rowsFeatures = new Array[SRow](1)
//    val training = Seq(Array(0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
//    for (i <- 0 until rowsFeatures.length) {
//      rowsFeatures(i) = new SRow((training(i)))
//    }
//
//    val schema = new StructType().add(new StructField("id", IntegerType, true))
//      .add(new StructField("hour", DoubleType, true))
//      .add(new StructField("mobile", DoubleType, true))
//      .add(new StructField("userFeatures", new VectorUDT, true))
//      .add(new StructField("clicked", DoubleType, true))
//    println("data schema : ",schema)
//    val dataset = new SDFrame(rowsFeatures)(schema)
//    println(dataset.schema, dataset.columns.length, dataset.columns(0),
//      dataset.getRow(0).get(0).toString, dataset.getRow(0).get(1).toString)
//    dataset.printSchema()
//    transModel.transform(dataset)
//  }
//}
