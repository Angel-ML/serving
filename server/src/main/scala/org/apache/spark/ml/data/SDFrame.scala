package org.apache.spark.ml.data

import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable.ListBuffer


class SDFrame(val rows: Array[SRow])(implicit val schema: StructType) extends Serializable {
  def columns: Array[String] = schema.fields.map(_.name)

  def printSchema(): Unit = println(schema.treeString)

  def setRow(i: Int, row: SRow): this.type = {
    rows(i) = row

    this
  }

  def getRow(i: Int): SRow = {
    rows(i)
  }

  def foreach(f: SRow => Unit): Unit = rows.foreach(f)

  def map(df: SRow => SRow): SDFrame = {
    val outSchema = new StructType(schema.toArray)
    val data = rows.map { row => df(row) }

    new SDFrame(data)(outSchema)
  }

  def map(df: SRow => SRow, sf: StructType => StructType): SDFrame = {
    val outSchema = sf(schema)
    val data = rows.map { row => df(row) }

    new SDFrame(data)(outSchema)
  }

  def withColum(udfCol: UDFCol): SDFrame = {
    val findRes = schema.zipWithIndex.collectFirst {
      case (sf, idx) if sf.name.equals(udfCol.name) => (sf, idx)
    }

    require(udfCol.check, "not all the columns is right!")

    if (findRes.isEmpty) {
      // append model
      val outSchema = new StructType(schema.toArray)
      SCol.mergeSchema(outSchema, udfCol.resSchema)

      val data = rows.map { case SRow(values: Array[Any]) =>
        val newValues = new Array[Any](values.length + 1)
        values.indices.foreach(idx => newValues(idx) = values(idx))
        newValues(values.length) = udfCol(values)
        SRow(newValues)
      }

      new SDFrame(data)(outSchema)
    } else {
      // replace model
      val (sf: StructField, idx: Int) = findRes.get

      val checked = udfCol.resSchema.head.dataType.getClass.getSimpleName.equalsIgnoreCase(
        sf.dataType.getClass.getSimpleName
      )

      require(checked, "data type not match!")

      val outSchema = new StructType(schema.toArray)
      val data = rows.map { case SRow(values: Array[Any]) =>
        val newValues = values.clone()
        newValues(idx) = udfCol(values)
        SRow(newValues)
      }

      new SDFrame(data)(outSchema)
    }
  }

  def select(cols: SCol*): SDFrame = {
    val checked = cols.forall(_.check)
    require(checked, "not all the columns is right!")

    val outSchema = cols.foldLeft(new StructType) {
      case (st, col) => SCol.mergeSchema(st, col.resSchema)
    }

    val data = rows.map { case SRow(values: Array[Any]) =>
      val row = new ListBuffer[Any]()
      cols.foreach {
        case col: StarCol =>
          col(values).asInstanceOf[Array[Any]].foreach(item => row.append(item))
        case col: SimpleCol =>
          row.append(col(values))
        case col: UDFCol =>
          row.append(col(values))
      }
      SRow(row.toArray)
    }

    new SDFrame(data)(outSchema)
  }

//  def select(colName: String*): SDFrame = {
//    select(colName.map(name => new SimpleCol(name)): _*)
//  }

  def filter(col: UDFCol): SDFrame = {
    val checked = col.check
    require(checked, "not all the columns is right!")

    val outSchema = new StructType(schema.toArray)

    val data = rows.filter { case SRow(values: Array[Any]) =>
      col(values).asInstanceOf[Boolean]
    }

    new SDFrame(data)(outSchema)
  }

  def na(): SDFrame = {
    val outSchema = new StructType(schema.toArray)
    val data = rows.filter { case SRow(values: Array[Any]) => !values.contains(null) }

    new SDFrame(data)(outSchema)
  }

  def drop(cols: SimpleCol*): SDFrame = {
    val idxs = new ListBuffer[Int]()
    val outSchema = new StructType()

    val idxSet = cols.map { col =>
      val colIdx = schema.zipWithIndex.collectFirst {
        case (sf, idx) if sf.name.equalsIgnoreCase(col.name) => idx
      }

      if (colIdx.isEmpty) {
        return null
      }

      colIdx.get
    }.toSet

    cols.indices.collect{
      case idx if !idxSet.contains(idx) =>
        idxs.append(idx)
        outSchema.add(schema(idx))
    }

    val data = rows.map { case SRow(values: Array[Any]) =>
      val newValue = new Array[Any](idxs.length)
      idxs.zipWithIndex.foreach {
        case (oldId, newId) => newValue(newId) = values(oldId)
      }
      SRow(newValue)
    }

    new SDFrame(data)(outSchema)
  }

//  def drop(colName: String*): SDFrame = {
//    drop(colName.map(name => new SimpleCol(name)): _*)
//  }
}
