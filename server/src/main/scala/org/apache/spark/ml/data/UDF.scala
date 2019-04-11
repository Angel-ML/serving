package org.apache.spark.ml.data

import org.apache.spark.sql.catalyst.AngelScalaReflection
import org.apache.spark.sql.types.DataType

import scala.reflect.runtime.universe.{TypeTag, typeTag}
import scala.util.Try

class UDF(val f: AnyRef, val dataType: DataType, val inputTypes: Option[Seq[DataType]],
          val isVarParams: Boolean = false) {
  private var nullable: Boolean = false
  private var inCols: Seq[SCol] = _

  def getNumInput: Int = {
    if (inputTypes.isEmpty) {
      0
    } else {
      inputTypes.get.length
    }
  }

  def asNullable(): this.type = {
    nullable = true

    this
  }

  def asNonNullable(): this.type = {
    nullable = false

    this
  }

  def isNullable: Boolean = nullable

  def getInCols: Seq[SCol] = inCols

  def apply(name: String, cols: SCol*): UDFCol = {
    inCols = cols

    val checkNotContainUDFCol = cols.forall {
      case _: StarCol => true
      case _: SimpleCol => true
      case _: UDFCol => false
    }

    require(checkNotContainUDFCol, "cols cannot contain UDFCol!")
    // todo
    new UDFCol(name, this)
  }
}

object UDF {
  def make[RT: TypeTag](f: () => RT): UDF = {
    val AngelScalaReflection.Schema(dataType, nullable) = AngelScalaReflection.schemaFor[RT]
    val inputTypes = Try(Nil).toOption
    val udf = new UDF(f, dataType, inputTypes)
    if (nullable) udf else udf.asNonNullable()
  }

  def make[RT: TypeTag, A1: TypeTag](f: A1 => RT, isVarParams: Boolean): UDF = {
    val AngelScalaReflection.Schema(dataType, nullable) = AngelScalaReflection.schemaFor[RT]
    val inputTypes = Try(AngelScalaReflection.schemaFor(typeTag[A1]).dataType :: Nil).toOption
    val udf = new UDF(f, dataType, inputTypes, isVarParams)
    if (nullable) udf else udf.asNonNullable()
  }

  def make[RT: TypeTag, A1: TypeTag, A2: TypeTag](f: (A1, A2) => RT, isVarParams: Boolean): UDF = {
    val AngelScalaReflection.Schema(dataType, nullable) = AngelScalaReflection.schemaFor[RT]
    val inputTypes = Try(AngelScalaReflection.schemaFor(typeTag[A1]).dataType ::
      AngelScalaReflection.schemaFor(typeTag[A2]).dataType :: Nil).toOption
    val udf = new UDF(f, dataType, inputTypes, isVarParams)
    if (nullable) udf else udf.asNonNullable()
  }

  def make[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](f: (A1, A2, A3) => RT, isVarParams: Boolean): UDF = {
    val AngelScalaReflection.Schema(dataType, nullable) = AngelScalaReflection.schemaFor[RT]
    val inputTypes = Try(AngelScalaReflection.schemaFor(typeTag[A1]).dataType ::
      AngelScalaReflection.schemaFor(typeTag[A2]).dataType ::
      AngelScalaReflection.schemaFor(typeTag[A3]).dataType :: Nil).toOption
    val udf = new UDF(f, dataType, inputTypes, isVarParams)
    if (nullable) udf else udf.asNonNullable()
  }
}
