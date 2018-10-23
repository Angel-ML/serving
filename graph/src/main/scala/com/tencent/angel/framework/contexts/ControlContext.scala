package com.tencent.angel.framework.contexts

import scala.collection.mutable
import com.tencent.angel.framework.{Graph, Operation}


class Controller(val controlInputs: Seq[Operation]) {
  val seenNodes = new mutable.HashSet[Operation]()

  def addOp(op: Operation): Unit = {
    seenNodes.add(op)
  }

  def opInGroup(op: Operation): Boolean = {
    seenNodes.contains(op)
  }
}


class ControlContext(graph: Graph) {
  private var controlDependencies = new mutable.ListBuffer[Controller]()

  def withControlDependencies(ctrlOps: Seq[Operation])(func: () => Unit): Unit = {
    if (ctrlOps != null && !ctrlOps.forall(op => graph.isElement(op))) {
      throw new Exception("all ops should come from the same graph!")
    }

    val validateCtrlOps = if (ctrlOps == null || ctrlOps.isEmpty) {
      ctrlOps
    } else {
      val current = new mutable.HashSet[Operation]()
      controlDependencies.foreach(ctrl => ctrl.controlInputs.foreach(current.add))
      ctrlOps.filter(op => !current.contains(op))
    }

    val controller = new Controller(validateCtrlOps)
    val oldStack: mutable.ListBuffer[Controller] = controlDependencies
    if (validateCtrlOps == null || validateCtrlOps.isEmpty) {
      controlDependencies = new mutable.ListBuffer[Controller]()
    }

    controlDependencies.append(controller)
    try {
      func()
    } finally {
      controlDependencies.remove(controlDependencies.size - 1)

      if (validateCtrlOps == null || validateCtrlOps.isEmpty) {
        controlDependencies = oldStack
      }
    }
  }

  def markAsSeen(op: Operation): Unit = controlDependencies.foreach { ctrl => ctrl.addOp(op) }

  def inferCtrlDependencies(inputOps: Seq[Operation]): List[Operation] = {
    if (!inputOps.forall(op => graph.isElement(op))) {
      throw new Exception("all ops should come from the same graph!")
    }

    val ctrlDeps = new mutable.HashSet[Operation]()
    controlDependencies.foreach { ctrl =>
      var dominated = false
      val iter = inputOps.iterator
      try {
        while (!dominated) {
          val op = iter.next()
          if (ctrl.opInGroup(op)) {
            dominated = true
          }
        }
      } catch {
        case ex: NoSuchElementException => // ex
        case _ => throw new Exception("Exception")
      }

      if (!dominated) {
        ctrl.controlInputs.filter(op => !inputOps.contains(op)).foreach(ctrlDeps.add)
      }
    }

    ctrlDeps.toList
  }

}
