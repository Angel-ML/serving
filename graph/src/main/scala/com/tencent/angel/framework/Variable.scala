package com.tencent.angel.framework


class Variable() {
  var tensor: Tensor = _

  def +(variable: Variable): Tensor = tensor + variable.tensor

  def -(variable: Variable): Tensor = tensor - variable.tensor

  def *(variable: Variable): Tensor = tensor * variable.tensor

  def /(variable: Variable): Tensor = tensor / variable.tensor

  def %(variable: Variable): Tensor = tensor % variable.tensor

  def >(variable: Variable): Tensor = tensor > variable.tensor

  def <(variable: Variable): Tensor = tensor < variable.tensor

  def >=(variable: Variable): Tensor = tensor >= variable.tensor

  def <=(variable: Variable): Tensor = tensor <= variable.tensor

  def !=(variable: Variable): Tensor = tensor != variable.tensor
}

object Variable {
  implicit def toTensor(variable: Variable): Tensor = variable.tensor
}
