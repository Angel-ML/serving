package org.apache.spark.ml.classification

import java.util
import java.util.{HashMap, Map}

import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors, Vector}
import org.apache.spark.ml.feature.utils.ModelUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object LogisticRegressionTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MLTest")
      .master("local")
      .getOrCreate()

    val training = spark.read.format("libsvm").load("data/sample_libsvm_data.txt")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val model = lr.fit(training)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")


    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = model.transform(training)
    indexedData.show()

    model.save("./models/spark/lr/1")

    val resdataset = trans(model)
    println(resdataset.schema, resdataset.columns.length, resdataset.columns(0),
      resdataset.getRow(0).get(0).toString, resdataset.getRow(0).get(1).toString)
    resdataset.printSchema()

    spark.stop()
  }

  def trans(model: LogisticRegressionModel): SDFrame = {
    val transModel = ModelUtils.transModel(model).asInstanceOf[LogisticRegressionServingModel]
    val rows = new Array[SRow](1)
    val size = 692
    val index = Array[Int](127,128,129,130,131,154,155,156,157,158,159,181,182,183,184,185,186,187,188,189,207,208,209,210,211,212,213,214,215,216,217,235,236,237,238,239,240,241,242,243,244,245,262,263,264,265,266,267,268,269,270,271,272,273,289,290,291,292,293,294,295,296,297,300,301,302,316,317,318,319,320,321,328,329,330,343,344,345,346,347,348,349,356,357,358,371,372,373,374,384,385,386,399,400,401,412,413,414,426,427,428,429,440,441,442,454,455,456,457,466,467,468,469,470,482,483,484,493,494,495,496,497,510,511,512,520,521,522,523,538,539,540,547,548,549,550,566,567,568,569,570,571,572,573,574,575,576,577,578,594,595,596,597,598,599,600,601,602,603,604,622,623,624,625,626,627,628,629,630,651,652,653,654,655,656,657)
    val value = Array[Double](51.0,159.0,253.0,159.0,50.0,48.0,238.0,252.0,252.0,252.0,237.0,54.0,227.0,253.0,252.0,239.0,233.0,252.0,57.0,6.0,10.0,60.0,224.0,252.0,253.0,252.0,202.0,84.0,252.0,253.0,122.0,163.0,252.0,252.0,252.0,253.0,252.0,252.0,96.0,189.0,253.0,167.0,51.0,238.0,253.0,253.0,190.0,114.0,253.0,228.0,47.0,79.0,255.0,168.0,48.0,238.0,252.0,252.0,179.0,12.0,75.0,121.0,21.0,253.0,243.0,50.0,38.0,165.0,253.0,233.0,208.0,84.0,253.0,252.0,165.0,7.0,178.0,252.0,240.0,71.0,19.0,28.0,253.0,252.0,195.0,57.0,252.0,252.0,63.0,253.0,252.0,195.0,198.0,253.0,190.0,255.0,253.0,196.0,76.0,246.0,252.0,112.0,253.0,252.0,148.0,85.0,252.0,230.0,25.0,7.0,135.0,253.0,186.0,12.0,85.0,252.0,223.0,7.0,131.0,252.0,225.0,71.0,85.0,252.0,145.0,48.0,165.0,252.0,173.0,86.0,253.0,225.0,114.0,238.0,253.0,162.0,85.0,252.0,249.0,146.0,48.0,29.0,85.0,178.0,225.0,253.0,223.0,167.0,56.0,85.0,252.0,252.0,252.0,229.0,215.0,252.0,252.0,252.0,196.0,130.0,28.0,199.0,252.0,252.0,253.0,252.0,252.0,233.0,145.0,25.0,128.0,252.0,253.0,252.0,141.0,37.0)

    val data: util.Map[String, Vector] = new util.HashMap[String, Vector]
    data.put(model.getFeaturesCol, Vectors.sparse(size, index, value))
//    for (i <- 0 until rows.length) {
//      rows(i) = new SRow(Array(Vectors.sparse(size, index, value)))
//    }

//    val schema = new StructType().add(new StructField(model.getFeaturesCol, new VectorUDT, true))
    val dataset = transModel.prepareData(data)
    transModel.transform(dataset)
  }
}
