package org.apache.spark.sql.types

import scala.reflect.runtime.universe.{Type, typeOf}

case class AnyType() extends DataType {
  override def defaultSize: Int = 4096

  override private[spark] def asNullable: DataType = this

  override def simpleString: String = "scala.Any"

  override def acceptsType(other: DataType): Boolean = other match {
    case _: AnyType => true
    case _ => false
  }
}

object AnyType {
  def tpe: Type = typeOf[AnyType]
}
