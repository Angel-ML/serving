package com.tencent.angel.framework

import java.util

import com.tencent.angel.utils.ProtoUtils
import com.tencent.angel.ops.MathOps

class Tensor(val op: Operation, val index: Int, val dtype: DataType) {
  val name = s"${op.name}:$index"
  val shape: TensorShape = TensorShape.unknownShape()
  val graph: Graph = op.graph
  lazy val rank: Int = shape.ndims
  val tensorPb: TensorProto = ProtoUtils.getTensorProto(dtype, shape.asProto())
  private val _consumer: util.ArrayList[Operation] = new util.ArrayList[Operation]()

  def addConsumer(cop: Operation): Unit = {
    _consumer.add(cop)
  }

  def consumer(): List[Operation] = {
    (0 until _consumer.size()).toList.map(i => _consumer.get(i))
  }

  def +(tensor: Tensor): Tensor = MathOps.add(this, tensor)

  def -(tensor: Tensor): Tensor = ???

  def *(tensor: Tensor): Tensor = ???

  def /(tensor: Tensor): Tensor = ???

  def %(tensor: Tensor): Tensor = ???

  def >(tensor: Tensor): Tensor = ???

  def <(tensor: Tensor): Tensor = ???

  def >=(tensor: Tensor): Tensor = ???

  def <=(tensor: Tensor): Tensor = ???

  def !=(tensor: Tensor): Tensor = ???
}

object Tensor {

}
