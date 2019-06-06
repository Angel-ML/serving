package org.apache.spark.ml.feature

import java.util

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object VectorSlicerServingTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val data = util.Arrays.asList(
      Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3)))),
      Row(Vectors.dense(-2.0, 2.3, 0.0))
    )

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val dataset = spark.createDataFrame(data, StructType(Array(attrGroup.toStructField())))

    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    slicer.setIndices(Array(1)).setNames(Array("f3"))

//     slicer.setIndices(Array(1, 2))
//    //or slicer.setNames(Array("f2", "f3"))
//
//    val output = slicer.transform(dataset)
//    output.show(false)

    val res = trans(slicer)
    println(res.schema, res.columns.length, res.columns(0),
      res.getRow(0).get(0).toString, res.getRow(0).get(1).toString)
    res.printSchema()
  }

  def trans(model: VectorSlicer): SDFrame = {
    val transModel = ModelUtils.transTransformer(model).asInstanceOf[VectorSlicerServing]
    val rowsFeatures = new Array[SRow](1)
//    for (i <- 0 until rowsFeatures.length) {
//      rowsFeatures(i) = new SRow(Array(Vectors.sparse(3, Seq((0, -2.0)))))
//    }

//    val defaultAttr = NumericAttribute.defaultAttr
//    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
//    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])
//    val schema = new StructType(Array(attrGroup.toStructField()))
//    println(schema.toString())
val data: util.Map[String, Vector] = new util.HashMap[String, Vector]
    data.put("userFeatures", Vectors.sparse(3, Seq((0, -2.0))))

    val dataset = transModel.prepareData(data)
    transModel.transform(dataset)
  }

  private def parseLine(line: String): (Float, Array[Int], Array[Double]) = {
    val indices = ArrayBuffer[Int]()
    val values = ArrayBuffer[Double]()
    val items = line.split("\\s+|,").map(_.trim)
    val label = items.head.toFloat

    for (item <- items.tail) {
      val ids = item.split(":")
      if (ids.length == 2) {
        indices += ids(0).toInt - 1 // convert 1-based indices to 0-based indices
        values += ids(1).toDouble
      }
      // check if indices are one-based and in ascending order
      var previous = -1
      var i = 0
      val indicesLength = indices.length
      while (i < indicesLength) {
        val current = indices(i)
        require(current > previous, "indices should be one-based and in ascending order")
        previous = current
        i += 1
      }
    }
    (label, indices.toArray, values.toArray)
  }
}
