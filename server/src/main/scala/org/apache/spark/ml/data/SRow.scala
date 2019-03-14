package org.apache.spark.ml.data


case class SRow(values: Array[Any]) {
  def length: Int = values.length

  def get(i: Int): Any = values(i)

  def get(is: Int*): Array[Any] = {
    is.toArray.map{ idx => values(idx)}
  }

  def set(i: Int, value: Any): this.type = {
    values(i) = value

    this
  }

  def copy(): SRow = this

  def toSeq: Seq[Any] = values.clone()
}
