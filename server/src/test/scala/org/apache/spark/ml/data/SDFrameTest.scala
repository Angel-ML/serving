package org.apache.spark.ml.data

import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.types.{BooleanType, DoubleType, StructField, StructType}

object SDFrameTest {
  def main(args: Array[String]): Unit = {
    val rowsFeatures = new Array[SRow](6)
    val size = 10
    val index = Array[Int](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val values = Seq(
      Array[Double](0.4551273600657362, 0.36644694351969087, -0.38256108933468047, -0.4458430198517267,
      0.33109790358914726, 0.8067445293443565, -0.2624341731773887, -0.44850386111659524, -0.07269284838169332, 0.5658035575800715),
      Array[Double](1.4551273600657362, 0.36644694351969087, -0.38256108933468047, -0.4458430198517267,
        0.33109790358914726, 0.8067445293443565, -0.2624341731773887, -0.44850386111659524, -0.07269284838169332, 0.5658035575800715),
      Array[Double](2.4551273600657362, 0.36644694351969087, -0.38256108933468047, -0.4458430198517267,
        0.33109790358914726, 0.8067445293443565, -0.2624341731773887, -0.44850386111659524, -0.07269284838169332, 0.5658035575800715),
      Array[Double](3.4551273600657362, 0.36644694351969087, -0.38256108933468047, -0.4458430198517267,
        0.33109790358914726, 0.8067445293443565, -0.2624341731773887, -0.44850386111659524, -0.07269284838169332, 0.5658035575800715),
      Array[Double](4.4551273600657362, 0.36644694351969087, -0.38256108933468047, -0.4458430198517267,
        0.33109790358914726, 0.8067445293443565, -0.2624341731773887, -0.44850386111659524, -0.07269284838169332, 0.5658035575800715),
      Array[Double](5.4551273600657362, 0.36644694351969087, -0.38256108933468047, -0.4458430198517267,
        0.33109790358914726, 0.8067445293443565, -0.2624341731773887, -0.44850386111659524, -0.07269284838169332, 0.5658035575800715),
      Array[Double](6.4551273600657362, 0.36644694351969087, -0.38256108933468047, -0.4458430198517267,
        0.33109790358914726, 0.8067445293443565, -0.2624341731773887, -0.44850386111659524, -0.07269284838169332, 0.5658035575800715))

    val bias = Seq(null, 0.1, 0.2, null, 0.3, 0.4, 0.5)
    val filter = Seq(true, false, true, true, false, true, true)

    for (i <- 0 until rowsFeatures.length) {
        rowsFeatures(i) = new SRow(Array(Vectors.sparse(size, index, values(i)), bias(i), filter(i)))
    }

    val schema = new StructType()
      .add(new StructField("features", new VectorUDT, true))
      .add(new StructField("bias", DoubleType, true))
      .add(new StructField("filter", BooleanType, true))
    val dataset = new SDFrame(rowsFeatures)(schema)
    println(dataset.printSchema(), dataset.columns.length, dataset.columns,
      dataset.getRow(0).get(0), dataset.getRow(0).get(1), dataset.getRow(0).get(2))

    //test na
    var output = dataset.na()
    println(output.printSchema(), output.columns.length, output.columns,
      output.getRow(0).get(0), output.getRow(0).get(1), output.getRow(0).get(2))

    //test filter
    val filterUDF = UDF.make[Boolean, Boolean](f => f, false)
    output = dataset.filter(filterUDF("filter_res", SCol("filter")))
    println(output.printSchema(), output.columns.length, output.columns,
      output.getRow(0).get(0), output.getRow(0).get(1), output.getRow(0).get(2))

    //test withColum
    output = dataset.withColum(filterUDF("filter_res", SCol("filter")))
    println(output.printSchema(), output.columns.length, output.columns,
      output.getRow(0).get(0), output.getRow(0).get(1), output.getRow(0).get(2), output.getRow(0).get(3))

    //test rename
    output = output.withColumnRenamed("filter_res", "filterRes")
    println(output.printSchema(), output.columns.length, output.columns,
      output.getRow(0).get(0), output.getRow(0).get(1), output.getRow(0).get(2), output.getRow(0).get(3))
    
    //test select
    val select = output.select(SCol("filter"))
    val selectAll = output.select(SCol())
    println("selectAll test", selectAll.printSchema(), selectAll.columns.length, selectAll.columns,
      selectAll.getRow(0).get(0))
    println("select test", select.printSchema(), select.columns.length, select.columns,
      select.getRow(0).get(0))


    //test drop
    println(output.printSchema())
    val drop = output.drop(new SimpleCol("filter"))
    println("drop test", drop.printSchema(), drop.columns.length, drop.columns,
      drop.getRow(0).get(0))
  }
}
