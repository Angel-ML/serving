package com.tencent.angel.framework

import com.tencent.angel.utils.ProtoUtils


class Dimension(val value: Long) {
  assert(value >= -1)

  def isCompatibleWith(other: Dimension): Boolean = {
    value == -1 || other.value == -1 || value == other.value
  }

  def assertIsCompatibleWith(other: Dimension): Unit = {
    if (! isCompatibleWith(other)) {
      throw new Exception(s"Dimensions $value and ${other.value} are not compatible")
    }
  }

  def mergeWith(other: Dimension): Dimension = {
    assertIsCompatibleWith(other)
    if (value == -1) {
      new Dimension(other.value)
    } else {
      new Dimension(value)
    }
  }

  def +(other: Dimension): Dimension = {
    if (value == -1 || other.value == -1) {
      new Dimension(-1)
    } else {
      new Dimension(value + other.value)
    }
  }

  def -(other: Dimension): Dimension = {
    if (value == -1 || other.value == -1) {
      new Dimension(-1)
    } else {
      assert(value >= other.value)
      new Dimension(value - other.value)
    }
  }

  def *(other: Dimension): Dimension = {
    if (value == -1 || other.value == -1) {
      new Dimension(-1)
    } else {
      new Dimension(value * other.value)
    }
  }

  def /(other: Dimension): Dimension = {
    if (value == -1 || other.value == -1) {
      new Dimension(-1)
    } else {
      new Dimension(value / other.value)
    }
  }

  def %(other: Dimension): Dimension = {
    if (value == -1 || other.value == -1) {
      new Dimension(-1)
    } else {
      new Dimension(value % other.value)
    }
  }

  def >(other: Dimension): Option[Boolean] = {
    if (value == -1 || other.value == -1) {
      None
    } else {
      Some(value > other.value)
    }
  }

  def >=(other: Dimension): Option[Boolean] = {
    if (value == -1 || other.value == -1) {
      None
    } else {
      Some(value >= other.value)
    }
  }

  def <(other: Dimension): Option[Boolean] = {
    if (value == -1 || other.value == -1) {
      None
    } else {
      Some(value < other.value)
    }
  }

  def <=(other: Dimension): Option[Boolean] = {
    if (value == -1 || other.value == -1) {
      None
    } else {
      Some(value <= other.value)
    }
  }

  def ==(other: Dimension): Option[Boolean] = {
    if (value == -1 || other.value == -1) {
      None
    } else {
      Some(value == other.value)
    }
  }

  def !=(other: Dimension): Option[Boolean] = {
    if (value == -1 || other.value == -1) {
      None
    } else {
      Some(value != other.value)
    }
  }
}

object Dimension {
  implicit def int2Dimension(value:Int): Dimension = new Dimension(value)
  implicit def long2Dimension(value:Long): Dimension = new Dimension(value)

  def as_dimension(value:Int): Dimension = new Dimension(value)
  def as_dimension(value:Long): Dimension = new Dimension(value)
}

class TensorShape(var dims: List[Dimension] = Nil) {
  assert(dims != null)

  // () : unknown
  // (k,) => 0: scalar, k: vector
  // (k1, k2, ...) => tensor
  def this(dims: Dimension*) = this(dims.toList)

  // dim: List[Long]
  def ndims: Int = if (dims.isEmpty) {
    -1  // unknown
  } else if (dims.length == 1 && dims.head.value == 0) {
    0  // scala
  } else {
    dims.length  // tensor
  }

  def numElements: Long = if (dims.length == 1 && dims.head.value == 0) {
    1
  } else if (isFullyDefined) {
    dims.foldLeft(1L)((last: Long, ele: Dimension) => last * ele.value)
  } else {
    -1
  }

  def isUnknown: Boolean = dims.isEmpty

  def isScalar: Boolean = dims.length == 1 && dims.head.value == 0

  def isVector: Boolean = dims.length == 1 && (dims.head.value == -1 || dims.head.value > 0)

  def isMatrix: Boolean = dims.length > 1

  def mergeWith(other: TensorShape): TensorShape = {
    if (dims.isEmpty) {
      other
    } else {
      try {
        assert(other.ndims == this.ndims)
        val newDims = dims.zip(other.dims).map{
          case (thisDim, otherDim) => thisDim.mergeWith(otherDim)
        }
        new TensorShape(newDims)
      } catch {
        case _ => throw new Exception("shape does not match!")
      }
    }
  }

  def concatenate(other: TensorShape): TensorShape = {
    if (ndims == -1 || other.ndims == -1) {
      TensorShape.unknownShape()
    } else {
      new TensorShape(dims ++ other.dims)
    }
  }

  def isCompatibleWith(other: TensorShape): Boolean = {
    if (ndims == other.ndims) {
      dims.zip(other.dims).forall{ case (d1, d2) => d1.isCompatibleWith(d2) }
    } else {
      false
    }
  }

  def isFullyDefined: Boolean = dims.map( dim => dim.value != -1).forall(x => x)

  def asList: List[Long] = dims.map(dim => dim.value)

  def asProto(): TensorShapeProto = ProtoUtils.getTensorShapeProto(asList)
}

object TensorShape {

  def unknownShape(ndims: Long = 0): TensorShape = {
    if (ndims == 0) {
      new TensorShape(List[Dimension](new Dimension(-1)))
    } else {
      val dims = (0 until ndims.toInt).toList.map( _ => new Dimension(-1) )
      new TensorShape(dims)
    }
  }

  def scalar(): TensorShape = new TensorShape(List[Dimension](new Dimension(0)))
  def vector(length:Long): TensorShape = new TensorShape(length)
  def matrix(rows:Long, cols: Long): TensorShape = new TensorShape(rows, cols)
}
