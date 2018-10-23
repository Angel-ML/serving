package com.tencent.angel.framework.contexts

import com.tencent.angel.framework.Operation
import com.tencent.angel.utils.ProtoUtils

class ContainerContext {
  private var container: String = ""

  def withContainer(name:String)(func: () => Unit): Unit = {
    assert (name != null && name.nonEmpty)
    val oldContainer = container

    container = name
    try {
      func()
    } finally {
      container = oldContainer
    }
  }

  def applyContainer(op:Operation): Unit = {
    if (op!= null && op.opDef.getIsStateful && container.nonEmpty) {
      (0 until op.opDef.getAttrCount).foreach{ i =>
        if (op.opDef.getAttr(i).getName == "container") {
          op.setAttr("container", ProtoUtils.getAttrValue(s=Some(container)))
        }
      }
    }
  }
}
