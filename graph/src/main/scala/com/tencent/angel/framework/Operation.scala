package com.tencent.angel.framework

import com.tencent.angel.utils.ProtoUtils


class Operation(val name: String,
                var nodeDef: NodeDef,
                val opDef: OpDef,
                val inputs: List[Tensor],
                val input_types: List[DataType],
                val control_inputs: List[Operation],
                val outputTypes: List[DataType],
                val graph: Graph) {
  val id: Int = graph.nextOpId()
  val outputs: List[Tensor] = outputTypes.zipWithIndex.map {
    case (dtype, i) => new Tensor(this, i, dtype)
  }

  var device: String = _

  def colocationGroups: List[String] = {
    val attrMap = nodeDef.getAttrMap
    if (attrMap.containsKey("_class")) {
      val bytesList = attrMap.get("_class").getList.getSList
      if (bytesList == null || bytesList.isEmpty) {
        List[String](s"loc:@$name")
      } else {
        (0 until bytesList.size()).toList
          .map(i => bytesList.get(i).toString)
          .filter(loc => loc.startsWith("loc:@"))
      }
    } else {
      List[String](s"loc:@$name")
    }
  }

  def setColocation(value:List[String]): Unit = {
    val attrValue = ProtoUtils.getAttrValue(list = Some(ProtoUtils.getListValue(value)))
    nodeDef = nodeDef.toBuilder.putAttr("_class", attrValue).build()
  }

  def getAttr(name:String): AttrValue = nodeDef.getAttrMap.get(name)

  def setAttr(name: String, value:AttrValue): Unit = {
    nodeDef = nodeDef.toBuilder.putAttr(name, value).build()
  }

  inputs.foreach { ts => ts.addConsumer(this) }
  graph.addOp(this)

}
