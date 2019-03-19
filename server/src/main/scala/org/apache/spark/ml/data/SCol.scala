package org.apache.spark.ml.data

import org.apache.spark.sql.types.{StructField, StructType}

abstract class SCol(val name: String) {
  val resSchema: StructType = new StructType

  def check(implicit schema: StructType): Boolean
}

object SCol {
  def apply(name: String): SCol = new SimpleCol(name)

  def apply(): SCol = new StarCol()

  def mergeSchema(s1: StructType, s2: StructType): StructType = {
    s2.foreach(sf => s1.add(sf))

    s1
  }
}

class StarCol(name: String = "*") extends SCol(name) {
  def apply(ipRow: Array[Any]): Any = ipRow

  override def check(implicit schema: StructType): Boolean = {
    // 1. prepare output schema and retuen
    schema.foreach(sf => resSchema.add(sf))
    true
  }
}

class SimpleCol(name: String) extends SCol(name) {
  private var idx: Int = -1

  def apply(ipRow: Array[Any]): Any = {
    assert(idx != -1)

    ipRow(idx)
  }

  override def check(implicit schema: StructType): Boolean = {
    // 1. check th input col in the schema
    val findRes = schema.zipWithIndex.collectFirst {
      case (sf, idx: Int) if sf.name.equalsIgnoreCase(name) => (sf, idx)
    }

    // 2. prepare output schema and retuen
    if (findRes.nonEmpty) {
      resSchema.add(findRes.get._1)
      idx = findRes.get._2
      true
    } else {
      false
    }
  }
}

class UDFCol(name: String, udf: UDF) extends SCol(name) {
  private var idxs: Seq[Int] = _

  def apply(ipRow: Array[Any]): Any = {
    udf.getNumInput match {
      case 0 =>
        val fun0 = udf.f.asInstanceOf[() => Any]
        fun0()
      case 1 =>
        val fun1 = udf.f.asInstanceOf[Any => Any]
        fun1(ipRow(idxs.head))
      case 2 =>
        val fun2 = udf.f.asInstanceOf[(Any, Any) => Any]
        fun2(ipRow(idxs.head), ipRow(idxs(1)))
      case 3 =>
        val fun3 = udf.f.asInstanceOf[(Any, Any, Any) => Any]
        fun3(ipRow(idxs.head), ipRow(idxs(1)), ipRow(idxs(2)))
      case _ => false
    }
  }

  override def check(implicit schema: StructType): Boolean = {
    // 1. check the input column is in the schema, and collect data
    val inColDTWithIdx = udf.getInCols.map { col: SCol =>
      val optSFIdx = schema.zipWithIndex.collectFirst {
        case (sf, idx) if sf.name.equalsIgnoreCase(col.name) => (sf.dataType, idx)
      }
      if (optSFIdx.isEmpty) {
        return false
      }
      optSFIdx.get
    }

    val (inColDTs, inColIdxs) = inColDTWithIdx.unzip
    idxs = inColIdxs

    // 2. check input data type
    if (udf.inputTypes.nonEmpty) {
      val isMatch = udf.inputTypes.get.zip(inColDTs).forall {
        case (dt1, dt2) => dt1.getClass.getSimpleName == dt2.getClass.getSimpleName
      }

      if (!isMatch) {
        return false
      }
    }

    // 3. prepare output schema
    resSchema.add(StructField(name, udf.dataType, udf.isNullable))

    true
  }
}
