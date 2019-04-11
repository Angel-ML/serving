package org.apache.spark.ml.data

import org.apache.spark.sql.types._

abstract class SCol(val name: String) {
  var resSchema: StructType

  def check(implicit schema: StructType): Boolean

  def setSchema(name: String, metadata: Metadata = Metadata.empty): SCol

  def setSchema(name: String, dataType: DataType, metadata: Metadata): SCol
}

object SCol {
  def apply(name: String): SCol = new SimpleCol(name)

  def apply(): SCol = new StarCol()

  def mergeSchema(s1: StructType, s2: StructType): StructType = {
    val list = new Array[StructField](s1.length + s2.length)
    s1.zipWithIndex.foreach { case (sf, idx) =>
      list(idx) = sf
    }
    s2.zipWithIndex.foreach { case (sf, idx) =>
      list(idx + s1.length) = sf
    }
    StructType(list)
  }
}

class StarCol(name: String = "*") extends SCol(name) {
  override var resSchema: StructType = _

  def apply(ipRow: Array[Any]): Any = ipRow

  override def check(implicit schema: StructType): Boolean = {
    // 1. prepare output schema and retuen
    schema.foreach(sf => {
      if (this.resSchema == null) {
        this.resSchema = new StructType().add(sf)
      } else {
        this.resSchema = this.resSchema.add(StructField(sf.name, sf.dataType, sf.nullable, sf.metadata))
      }
    })
    true
  }

  override def setSchema(name: String, metadata: Metadata = Metadata.empty): StarCol = ???

  override def setSchema(name: String, dataType: DataType, metadata: Metadata): SCol = ???
}

class SimpleCol(name: String) extends SCol(name) {
  private var idx: Int = -1
  override var resSchema: StructType = _

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
      if (this.resSchema == null) {
        this.resSchema = new StructType().add(findRes.get._1)
      } else {
        this.resSchema = this.resSchema.add(
          StructField(findRes.get._1.name, findRes.get._1.dataType, findRes.get._1.nullable, findRes.get._1.metadata))
      }
//      resSchema.add(findRes.get._1)
      idx = findRes.get._2
      true
    } else {
      false
    }
  }

  override def setSchema(
                          name: String,
                          metadata: Metadata = Metadata.empty): UDFCol = ???

  override def setSchema(name: String, dataType: DataType, metadata: Metadata): SCol = {
    if (this.resSchema == null) {
      this.resSchema = new StructType().add(new StructField(name, dataType, true, metadata))
    } else {
      val iter = this.resSchema.iterator
      iter.dropWhile(s => s.name == name)
      this.resSchema = this.resSchema.add(StructField(name, dataType, true, metadata))
    }
    this
  }
}

class UDFCol(name: String, udf: UDF) extends SCol(name) {
  private var idxs: Seq[Int] = _
  override var resSchema: StructType = _

  def apply(ipRow: Array[Any]): Any = {
    udf.getNumInput match {
      case 0 =>
        val fun0 = udf.f.asInstanceOf[() => Any]
        fun0()
      case 1 =>
        val fun1 = udf.f.asInstanceOf[Any => Any]
        if (udf.isVarParams) {
          fun1(idxs.map(i => ipRow(i)))
        } else {
          fun1(ipRow(idxs.head))
        }
      case 2 =>
        val fun2 = udf.f.asInstanceOf[(Any, Any) => Any]
        if (udf.isVarParams) {
          fun2(ipRow(idxs.head), idxs.tail.map(i => ipRow(i)))
        } else {
          fun2(ipRow(idxs.head), ipRow(idxs(1)))
        }
      case 3 =>
        val fun3 = udf.f.asInstanceOf[(Any, Any, Any) => Any]
        if (udf.isVarParams) {
          fun3(ipRow(idxs.head), ipRow(idxs(1)), idxs.tail.tail.map(i => ipRow(i)))
        } else {
          fun3(ipRow(idxs.head), ipRow(idxs(1)), ipRow(idxs(2)))
        }
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
      val isMatch = if (inColDTs.length == udf.inputTypes.get.length && !udf.isVarParams) {
        udf.inputTypes.get.zip(inColDTs).forall {
          case (dt1, dt2) =>
            dt1.getClass.getSimpleName == dt2.getClass.getSimpleName
        }
      } else if (inColDTs.length == udf.inputTypes.get.length - 1 && udf.isVarParams) {
        // no params for VarParams
        inColDTs.zipWithIndex.forall {
          case (dt2, idx) =>
            val dt1 = udf.inputTypes.get.apply(idx)
            dt1.getClass.getSimpleName == dt2.getClass.getSimpleName
        }
      } else if (inColDTs.length >= udf.inputTypes.get.length && udf.isVarParams) {
        // Note: varParam must come last
        val lastParamIdx = udf.inputTypes.get.length - 1
        udf.inputTypes.get.zipWithIndex.forall {
          case (dt1, idx) if idx != lastParamIdx =>
            val dt2 = inColDTs(idx)
            dt1.getClass.getSimpleName == dt2.getClass.getSimpleName
          case (ArrayType(eleType, _), idx) if idx == lastParamIdx =>
            val dt1Name = eleType.getClass.getSimpleName
            if (dt1Name == classOf[AnyType].getSimpleName) {
              true
            } else {
              (idx until inColDTs.length).forall { i =>
                inColDTs(i).getClass.getSimpleName == dt1Name
              }
            }
        }
      } else {
        false
      }

      if (!isMatch) {
        return false
      }
    }

    // 3. prepare output schema
    this.resSchema = new StructType().add(StructField(name, udf.dataType, udf.isNullable))

    true
  }

  override def setSchema(
                          name: String,
                          metadata: Metadata = Metadata.empty): UDFCol = {
    if (this.resSchema == null) {
      this.resSchema = new StructType().add(StructField(name, udf.dataType, udf.isNullable, metadata))
    } else {
      val iter = this.resSchema.iterator
      iter.dropWhile(s => s.name == name)
      this.resSchema = this.resSchema.add(StructField(name, udf.dataType, udf.isNullable, metadata))
    }
    this
  }

  override def setSchema(name: String, dataType: DataType, metadata: Metadata): SCol = {
    if (this.resSchema == null) {
      this.resSchema = new StructType().add(StructField(name, dataType, udf.isNullable, metadata))
    } else {
      val iter = this.resSchema.iterator
      iter.dropWhile(s => s.name == name)
      this.resSchema = this.resSchema.add(StructField(name, dataType, udf.isNullable, metadata))
    }
    this
  }
}
