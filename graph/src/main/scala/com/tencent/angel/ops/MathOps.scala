package com.tencent.angel.ops

import com.tencent.angel.framework._
import com.tencent.angel.utils.ProtoUtils

object MathOps {
  private val opLib = initOps()

  def add(a: Tensor, b: Tensor, name: String = "Add"): Tensor = {
    val graph = Graph.getGraph
    // 1. ensure all the input in the same graph
    if (graph != a.graph || graph != b.graph) {
      throw new Exception("the tensors in inputs should in the same graph!")
    }

    // 2. make sure a and b can add, the shape is compatible

    // 3. get the OpDef
    val opDef: OpDef = opLib.getOpDef("Add")

    // 4. create a NodeDef
    val inputs = List[Tensor](a, b)
    val opName = graph.getName(name)
    val nodeDef: NodeDef = ProtoUtils.getNodeDef(opName, "Add", inputs.map(ip => ip.name))

    // 5. create Operation
    val outputTypes = List[DataType](a.dtype)
    val controlInputs: List[Operation] = null
    val addOp = graph.createOp(nodeDef, opDef, inputs, controlInputs, outputTypes)

    // 6. return a tensor
    addOp.outputs.head
  }

  private def initOps(): OpLibrary = {
    new OpLibrary("math_ops.pb")
  }
}
